// src/service_matching/service_url.rs
// Enhanced URL matching with query parameter handling and improved slug analysis

use anyhow::Result;
use log::{debug, warn};
use url::Url as StdUrl;
use std::collections::HashMap;

use crate::models::{MatchMethodType, MatchValues, ServiceId, ServiceUrlMatchValue};

use super::types::{NormalizedServiceUrl, ServiceDomainType, ServiceInfo, ServiceUrlMatch};

// Enhanced configuration constants for URL matching
const CONFIDENCE_DOMAIN_ONLY: f64 = 0.70;
const CONFIDENCE_DOMAIN_PLUS_ONE_SLUG: f64 = 0.80;
const CONFIDENCE_DOMAIN_PLUS_TWO_SLUGS: f64 = 0.88;
const CONFIDENCE_DOMAIN_PLUS_THREE_OR_MORE_SLUGS: f64 = 0.92;
const CONFIDENCE_DOMAIN_FULL_PATH_MATCH: f64 = 0.96;
const CONFIDENCE_SUBDOMAIN_BONUS: f64 = 0.05;
const CONFIDENCE_HTTPS_BONUS: f64 = 0.02;

// NEW: Query parameter handling constants
const CONFIDENCE_QUERY_PARAM_BONUS: f64 = 0.03; // Bonus for matching significant query params
const CONFIDENCE_QUERY_PARAM_PENALTY: f64 = 0.05; // Penalty for conflicting significant query params

// NEW: Fragment handling constants
const CONFIDENCE_FRAGMENT_MATCH_BONUS: f64 = 0.02; // Bonus for matching fragments
const CONFIDENCE_FRAGMENT_DIFFERENT_PENALTY: f64 = 0.01; // Small penalty for different fragments on same page

// NEW: Weighted slug matching constants
const FIRST_SLUG_WEIGHT: f64 = 1.0;
const SECOND_SLUG_WEIGHT: f64 = 0.8;
const THIRD_SLUG_WEIGHT: f64 = 0.6;
const REMAINING_SLUG_WEIGHT: f64 = 0.4;

/// Enhanced URL matching with query parameter and fragment handling
pub fn match_services_by_url(service1: &ServiceInfo, service2: &ServiceInfo) -> Option<ServiceUrlMatch> {
    let url1_str = service1.url.as_ref()?;
    let url2_str = service2.url.as_ref()?;

    // Normalize both URLs using the enhanced normalizer
    let normalized1 = normalize_service_url(url1_str)?;
    let normalized2 = normalize_service_url(url2_str)?;

    // Check for ignored domains
    if is_ignored_service_domain(&normalized1.domain) || is_ignored_service_domain(&normalized2.domain) {
        debug!("Skipping URL match due to ignored domain: {} or {}", normalized1.domain, normalized2.domain);
        return None;
    }

    // Domains must match for any further consideration
    if normalized1.domain != normalized2.domain {
        return None;
    }

    // Calculate enhanced confidence based on URL similarity
    let confidence = calculate_enhanced_service_url_confidence(&normalized1, &normalized2);

    // Apply minimum threshold for a match
    if confidence < CONFIDENCE_DOMAIN_ONLY - 0.01 {
        return None;
    }

    let matching_slug_count = count_weighted_matching_slugs(&normalized1.path_slugs, &normalized2.path_slugs);

    // Ensure canonical ordering of service IDs
    let (ordered_service1_id, ordered_service2_id, ordered_url1_str, ordered_url2_str) =
        if service1.service_id.0 <= service2.service_id.0 {
            (&service1.service_id, &service2.service_id, url1_str, url2_str)
        } else {
            (&service2.service_id, &service1.service_id, url2_str, url1_str)
        };

    Some(ServiceUrlMatch {
        service_id_1: ordered_service1_id.clone(),
        service_id_2: ordered_service2_id.clone(),
        confidence_score: confidence.min(1.0),
        match_values: MatchValues::ServiceUrl(ServiceUrlMatchValue {
            original_url1: ordered_url1_str.clone(),
            original_url2: ordered_url2_str.clone(),
            normalized_shared_domain: normalized1.domain.clone(),
            matching_slug_count: matching_slug_count as usize, // Convert from f64 to usize for compatibility
        }),
    })
}

/// Enhanced URL normalization with query parameter and fragment handling
pub fn normalize_service_url(url_str: &str) -> Option<NormalizedServiceUrl> {
    let trimmed_url = url_str.trim();
    if trimmed_url.is_empty() || trimmed_url.starts_with("mailto:") || trimmed_url.starts_with("tel:") {
        return None;
    }

    // Add scheme if missing, defaulting to https
    let url_with_scheme = if !trimmed_url.contains("://") {
        format!("https://{}", trimmed_url)
    } else {
        trimmed_url.to_string()
    };

    let parsed_url = StdUrl::parse(&url_with_scheme).ok()?;
    let host = parsed_url.host_str()?.to_lowercase();

    // Extract domain and subdomain
    let (domain, subdomain) = extract_domain_and_subdomain(&host);

    // Validate domain
    if domain.is_empty() || !domain.contains('.') || is_ip_address(&domain) {
        debug!("Invalid domain for matching: {}", domain);
        return None;
    }

    // Extract and normalize path slugs
    let path_slugs: Vec<String> = parsed_url
        .path_segments()
        .map_or_else(Vec::new, |segments| {
            segments
                .filter(|s| !s.is_empty() && s.len() < 100 && is_valid_slug_enhanced(s))
                .map(|s| s.to_lowercase())
                .collect()
        });

    // NEW: Extract and normalize significant query parameters
    let significant_query_params = extract_significant_query_params(&parsed_url);

    // NEW: Extract and normalize fragment
    let fragment = parsed_url
        .fragment()
        .map(|f| normalize_fragment(f))
        .filter(|f| !f.is_empty());

    let is_https = parsed_url.scheme() == "https";

    Some(NormalizedServiceUrl {
        domain,
        path_slugs,
        original_url: url_str.to_string(),
        domain_type: categorize_service_domain(&host),
        subdomain,
        is_https,
        // NEW: Add query params and fragment to the struct (you'll need to update NormalizedServiceUrl)
        significant_query_params: Some(significant_query_params),
        fragment,
    })
}

/// NEW: Extract significant query parameters that might differentiate services
fn extract_significant_query_params(url: &StdUrl) -> HashMap<String, String> {
    let mut significant_params = HashMap::new();
    
    // Define query parameters that are significant for service differentiation
    let significant_param_names = [
        "service", "category", "type", "department", "program", "service_id",
        "location", "area", "region", "site", "branch", "office",
        "lang", "language", "locale", // Language preferences
        "page", "section", "tab", "view", // Page sections that might indicate different services
    ];
    
    for (key, value) in url.query_pairs() {
        let key_lower = key.to_lowercase();
        if significant_param_names.iter().any(|&param| key_lower.contains(param)) {
            // Normalize the value - handle URL encoding and separators separately
            let mut normalized_value = value.to_lowercase();
            
            // Replace URL-encoded space first
            normalized_value = normalized_value.replace("%20", " ");
            
            // Then replace other separators
            normalized_value = normalized_value.replace(['-', '_'], " ");
            
            let normalized_value = normalized_value.trim().to_string();
            
            if !normalized_value.is_empty() && normalized_value.len() < 50 { // Reasonable length limit
                significant_params.insert(key_lower, normalized_value);
            }
        }
    }
    
    significant_params
}

/// NEW: Normalize fragment identifiers
fn normalize_fragment(fragment: &str) -> String {
    fragment
        .to_lowercase()
        .replace(['-', '_'], " ")
        .split_whitespace()
        .filter(|word| word.len() > 1) // Remove single characters
        .collect::<Vec<_>>()
        .join(" ")
}

/// Enhanced slug validation with contextual awareness
fn is_valid_slug_enhanced(slug: &str) -> bool {
    if slug.len() < 2 || slug.len() > 70 {
        return false;
    }
    
    // Enhanced file extension detection
    if slug.contains('.') {
        let common_extensions = [
            ".html", ".htm", ".php", ".asp", ".aspx", ".jsp", ".cgi", 
            ".pdf", ".doc", ".docx", ".xml", ".json", ".js", ".css",
            ".png", ".jpg", ".jpeg", ".gif", ".svg", ".ico", ".webp",
            ".mp3", ".mp4", ".avi", ".mov", ".zip", ".rar", ".exe"
        ];
        if common_extensions.iter().any(|ext| slug.ends_with(ext)) {
            return false;
        }
    }
    
    // Skip numeric-only slugs that are likely IDs, but with better heuristics
    if slug.chars().all(|c| c.is_ascii_digit()) {
        // Keep short numbers (might be meaningful like route numbers)
        if slug.len() <= 3 {
            return true;
        }
        // Skip long numeric strings (likely IDs)
        if slug.len() > 4 {
            return false;
        }
    }
    
    // Enhanced generic slug detection with context awareness
    let highly_generic_slugs = [
        "index", "default", "home", "main", "content", "page", "item", 
        "view", "show", "list", "all", "general", "common", "basic"
    ];
    
    let moderately_generic_slugs = [
        "about", "contact", "info", "news", "events", "resources", 
        "help", "support", "faq", "search", "admin", "login"
    ];
    
    // Reject highly generic short slugs
    if slug.len() <= 5 && highly_generic_slugs.contains(&slug) {
        return false;
    }
    
    // Be more lenient with moderately generic slugs if they're longer
    if slug.len() <= 3 && moderately_generic_slugs.contains(&slug) {
        return false;
    }
    
    // Keep slugs that contain meaningful service-related terms
    let service_keywords = [
        "service", "program", "clinic", "center", "office", "department",
        "food", "health", "legal", "housing", "employment", "education",
        "mental", "substance", "emergency", "crisis", "assistance"
    ];
    
    if service_keywords.iter().any(|&keyword| slug.contains(keyword)) {
        return true;
    }
    
    true
}

/// NEW: Calculate weighted matching score for slugs (giving more weight to earlier slugs)
fn count_weighted_matching_slugs(slugs1: &[String], slugs2: &[String]) -> f64 {
    let mut weighted_score = 0.0;
    let max_len = slugs1.len().min(slugs2.len());
    
    for i in 0..max_len {
        if slugs1[i] == slugs2[i] {
            let weight = match i {
                0 => FIRST_SLUG_WEIGHT,
                1 => SECOND_SLUG_WEIGHT,
                2 => THIRD_SLUG_WEIGHT,
                _ => REMAINING_SLUG_WEIGHT,
            };
            weighted_score += weight;
        } else {
            break; // Stop at first non-match since we're looking for sequential matches
        }
    }
    
    weighted_score
}

/// Enhanced confidence calculation with query parameters and fragments
fn calculate_enhanced_service_url_confidence(url1: &NormalizedServiceUrl, url2: &NormalizedServiceUrl) -> f64 {
    let weighted_slug_score = count_weighted_matching_slugs(&url1.path_slugs, &url2.path_slugs);
    
    // Base confidence from slug matching
    let mut confidence = if !url1.path_slugs.is_empty() && url1.path_slugs == url2.path_slugs {
        CONFIDENCE_DOMAIN_FULL_PATH_MATCH
    } else {
        // Convert weighted score to confidence levels
        match weighted_slug_score as usize {
            0 => CONFIDENCE_DOMAIN_ONLY,
            1 => CONFIDENCE_DOMAIN_PLUS_ONE_SLUG,
            2 => CONFIDENCE_DOMAIN_PLUS_TWO_SLUGS,
            _ => CONFIDENCE_DOMAIN_PLUS_THREE_OR_MORE_SLUGS,
        }
    };
    
    // Apply weighted slug bonus for partial matches
    if weighted_slug_score > 0.0 && weighted_slug_score < 3.0 {
        let partial_bonus = (weighted_slug_score % 1.0) * 0.02; // Small bonus for weighted partial matches
        confidence += partial_bonus;
    }

    // Subdomain matching bonus
    if let (Some(sub1), Some(sub2)) = (&url1.subdomain, &url2.subdomain) {
        if sub1 == sub2 && sub1 != "www" {
            confidence += CONFIDENCE_SUBDOMAIN_BONUS;
        }
    }

    // HTTPS bonus
    if url1.is_https && url2.is_https {
        confidence += CONFIDENCE_HTTPS_BONUS;
    }

    // NEW: Query parameter analysis
    if let (Some(ref params1), Some(ref params2)) = (&url1.significant_query_params, &url2.significant_query_params) {
        let param_adjustment = calculate_query_param_adjustment(params1, params2);
        confidence += param_adjustment;
    }

    // NEW: Fragment analysis
    match (&url1.fragment, &url2.fragment) {
        (Some(frag1), Some(frag2)) => {
            if frag1 == frag2 {
                confidence += CONFIDENCE_FRAGMENT_MATCH_BONUS;
            } else if are_related_fragments(frag1, frag2) {
                // Small penalty for different but related fragments (e.g., different sections of same service)
                confidence -= CONFIDENCE_FRAGMENT_DIFFERENT_PENALTY;
            }
        },
        (None, None) => {
            // No fragments, no adjustment
        },
        _ => {
            // One has fragment, other doesn't - slight penalty
            confidence -= CONFIDENCE_FRAGMENT_DIFFERENT_PENALTY * 0.5;
        }
    }

    // Apply domain-type specific adjustments
    confidence = apply_domain_type_adjustments(confidence, &url1.domain_type, weighted_slug_score as usize);
    
    confidence.max(0.0).min(1.0)
}

/// NEW: Calculate adjustment based on query parameter matching
fn calculate_query_param_adjustment(params1: &HashMap<String, String>, params2: &HashMap<String, String>) -> f64 {
    if params1.is_empty() && params2.is_empty() {
        return 0.0;
    }
    
    let mut adjustment = 0.0;
    let all_keys: std::collections::HashSet<_> = params1.keys().chain(params2.keys()).collect();
    
    for key in all_keys {
        match (params1.get(key), params2.get(key)) {
            (Some(val1), Some(val2)) => {
                if val1 == val2 {
                    adjustment += CONFIDENCE_QUERY_PARAM_BONUS; // Bonus for matching param
                } else if are_related_param_values(val1, val2) {
                    adjustment += CONFIDENCE_QUERY_PARAM_BONUS * 0.5; // Smaller bonus for related values
                } else {
                    adjustment -= CONFIDENCE_QUERY_PARAM_PENALTY; // Penalty for conflicting values
                }
            },
            (Some(_), None) | (None, Some(_)) => {
                // One has parameter, other doesn't - small penalty
                adjustment -= CONFIDENCE_QUERY_PARAM_PENALTY * 0.3;
            },
            (None, None) => {
                // Both don't have this parameter - no adjustment needed
                // This case is handled by the outer loop only including keys that exist in at least one map
            }
        }
    }
    
    adjustment
}


/// NEW: Check if query parameter values are related
fn are_related_param_values(val1: &str, val2: &str) -> bool {
    // Check for common variations
    let normalized1 = val1.replace(['-', '_'], " ");
    let normalized2 = val2.replace(['-', '_'], " ");
    
    if normalized1 == normalized2 {
        return true;
    }
    
    // Check for synonyms in service categories
    let service_synonyms = [
        ("food", "nutrition"), ("food", "meals"), 
        ("housing", "shelter"), ("housing", "residential"),
        ("health", "medical"), ("health", "healthcare"),
        ("mental health", "behavioral health"), ("mental health", "counseling"),
        ("legal", "law"), ("legal", "attorney"),
        ("employment", "job"), ("employment", "career"),
    ];
    
    for (syn1, syn2) in &service_synonyms {
        if (val1.contains(syn1) && val2.contains(syn2)) || 
           (val1.contains(syn2) && val2.contains(syn1)) {
            return true;
        }
    }
    
    false
}

/// NEW: Check if fragments are related (different sections of same service)
fn are_related_fragments(frag1: &str, frag2: &str) -> bool {
    // Common related fragment patterns
    let related_patterns = [
        ("about", "info"), ("about", "overview"),
        ("contact", "location"), ("contact", "hours"),
        ("services", "programs"), ("services", "offerings"),
        ("eligibility", "requirements"), ("eligibility", "criteria"),
        ("apply", "application"), ("apply", "enroll"),
        ("hours", "schedule"), ("hours", "times"),
    ];
    
    for (pat1, pat2) in &related_patterns {
        if (frag1.contains(pat1) && frag2.contains(pat2)) || 
           (frag1.contains(pat2) && frag2.contains(pat1)) {
            return true;
        }
    }
    
    false
}

/// Extract domain and subdomain with enhanced TLD handling
fn extract_domain_and_subdomain(host: &str) -> (String, Option<String>) {
    let host_no_www = host.strip_prefix("www.").unwrap_or(host);
    let parts: Vec<&str> = host_no_www.split('.').collect();
    
    if parts.len() < 2 {
        return (host_no_www.to_string(), None);
    }

    // Enhanced TLD handling for common multi-part TLDs
    let multi_part_tlds = [
        "co.uk", "org.uk", "ac.uk", "gov.uk", "com.au", "org.au", "gov.au",
        "co.nz", "org.nz", "gov.nz", "co.za", "org.za", "gov.za",
        "com.br", "org.br", "gov.br", "co.jp", "or.jp", "go.jp"
    ];
    
    // Check for multi-part TLD
    if parts.len() >= 3 {
        let potential_tld = format!("{}.{}", parts[parts.len()-2], parts[parts.len()-1]);
        if multi_part_tlds.contains(&potential_tld.as_str()) {
            let domain = if parts.len() == 3 {
                host_no_www.to_string()
            } else {
                parts[parts.len()-3..].join(".")
            };
            let subdomain = if parts.len() > 3 {
                Some(parts[..parts.len()-3].join("."))
            } else {
                None
            };
            return (domain, subdomain);
        }
    }
    
    // Standard two-part TLD handling
    if parts.len() == 2 {
        (host_no_www.to_string(), None)
    } else {
        let domain = parts[parts.len()-2..].join(".");
        let subdomain_parts = &parts[..parts.len()-2];
        let subdomain = if subdomain_parts.is_empty() { 
            None 
        } else { 
            Some(subdomain_parts.join(".")) 
        };
        (domain, subdomain)
    }
}

/// Enhanced domain type adjustments with more granular handling
fn apply_domain_type_adjustments(mut confidence: f64, domain_type: &ServiceDomainType, matching_slugs: usize) -> f64 {
    match domain_type {
        ServiceDomainType::Government => {
            if matching_slugs >= 1 { 
                confidence = (confidence * 1.08).min(0.99); // Government sites often have meaningful paths
            } else {
                confidence *= 0.95; // Small penalty for gov domains with no path match
            }
        },
        ServiceDomainType::Education => {
            if matching_slugs >= 1 { 
                confidence = (confidence * 1.05).min(0.99); 
            }
        },
        ServiceDomainType::Healthcare => {
             if matching_slugs >= 1 { 
                 confidence = (confidence * 1.04).min(0.99); 
             }
        },
        ServiceDomainType::CloudService | ServiceDomainType::GenericPlatform => {
            // For these, path similarity is crucial
            if matching_slugs == 0 { 
                confidence *= 0.75; // Strong penalty for cloud/platform domains with no path match
            } else if matching_slugs == 1 { 
                confidence *= 0.85; 
            } else {
                confidence *= 0.95; // Still slight penalty even with matches
            }
        },
        ServiceDomainType::Social => {
            confidence *= 0.80; // Social media links are less reliable
        },
        ServiceDomainType::Commercial => {
            // Commercial domains need good path matches for confidence
            if matching_slugs == 0 {
                confidence *= 0.90;
            }
        },
        ServiceDomainType::Other => {
            // No specific adjustment for Other category
        }
    }
    confidence
}

/// Count traditional sequential matching slugs (for compatibility)
fn count_matching_slugs(slugs1: &[String], slugs2: &[String]) -> usize {
    slugs1.iter().zip(slugs2.iter()).take_while(|(s1, s2)| s1 == s2).count()
}

/// Enhanced service domain categorization
pub fn categorize_service_domain(host: &str) -> ServiceDomainType {
    // Government domains (enhanced)
    if host.ends_with(".gov") || host.ends_with(".mil") || 
       host.contains(".gov.") || host.contains(".state.") || 
       host.contains("city.") || host.contains("county.") {
        return ServiceDomainType::Government;
    }
    
    // Education domains (enhanced)
    if host.ends_with(".edu") || host.contains(".edu.") ||
       host.contains("k12") || host.contains("university") || 
       host.contains("college") || host.contains("school.") ||
       host.contains("district.") {
        return ServiceDomainType::Education;
    }
    
    // Healthcare domains (enhanced)
    let healthcare_keywords = ["health", "hospital", "clinic", "medical", "care", "therapy", "dental", "mental"];
    if healthcare_keywords.iter().any(|&keyword| host.contains(keyword)) ||
       (host.ends_with(".org") && healthcare_keywords.iter().any(|&keyword| host.contains(keyword))) {
        return ServiceDomainType::Healthcare;
    }
    
    // Cloud and platform domains
    if is_cloud_service_domain(host) { 
        return ServiceDomainType::CloudService; 
    }
    if is_generic_platform_domain(host) { 
        return ServiceDomainType::GenericPlatform; 
    }
    if is_social_domain(host) { 
        return ServiceDomainType::Social; 
    }
    
    // Commercial domains
    if host.ends_with(".com") || host.ends_with(".net") || host.ends_with(".biz") || 
       host.ends_with(".co") || host.ends_with(".io") || host.ends_with(".app") {
        return ServiceDomainType::Commercial;
    }
    
    // Default for .org and others
    ServiceDomainType::Other
}

/// Enhanced domain ignore list with better coverage
pub fn is_ignored_service_domain(domain_to_check: &str) -> bool {
    // Enhanced social media domains
    const SOCIAL_DOMAINS: &[&str] = &[
        "facebook.com", "fb.com", "twitter.com", "x.com", "instagram.com", 
        "linkedin.com", "youtube.com", "youtu.be", "tiktok.com", "pinterest.com", 
        "snapchat.com", "reddit.com", "wa.me", "t.me", "discord.gg", "discord.com", 
        "medium.com", "whatsapp.com", "telegram.org", "signal.org"
    ];
    
    // Enhanced generic platforms
    const GENERIC_PLATFORMS: &[&str] = &[
        "eventbrite.com", "meetup.com", "wordpress.com", "blogspot.com", "tumblr.com",
        "wix.com", "wixsite.com", "squarespace.com", "weebly.com", "jimdo.com",
        "godaddy.com", "godaddysites.com", "sites.google.com", "forms.gle",
        "docs.google.com", "drive.google.com", "dropbox.com", "box.com", 
        "onedrive.live.com", "icloud.com", "zoom.us", "teams.microsoft.com",
        "surveymonkey.com", "typeform.com", "carrd.co", "linktr.ee", 
        "sendgrid.net", "mailchimp.com", "constantcontact.com", "hubspot.com",
        "salesforce.com", "googleusercontent.com", "amazonaws.com"
    ];
    
    // Enhanced URL shorteners
    const URL_SHORTENERS: &[&str] = &[
        "bit.ly", "tinyurl.com", "t.co", "ow.ly", "goo.gl", "is.gd", 
        "buff.ly", "short.io", "rebrand.ly", "tiny.cc", "tr.im", "url.ie"
    ];
    
    let all_ignored = [SOCIAL_DOMAINS, GENERIC_PLATFORMS, URL_SHORTENERS].concat();
    all_ignored.iter().any(|&ignored_domain| {
        domain_to_check == ignored_domain || 
        domain_to_check.ends_with(&format!(".{}", ignored_domain))
    })
}

/// Enhanced cloud service domain detection
fn is_cloud_service_domain(host: &str) -> bool {
    const CLOUD_HOSTS: &[&str] = &[
        // AWS
        "amazonaws.com", "aws.com", "s3.amazonaws.com", "cloudfront.net",
        // Azure
        "azurewebsites.net", "azureedge.net", "windows.net", "azure.com",
        // Google Cloud
        "cloud.goog", "appspot.com", "storage.googleapis.com", "firebaseapp.com", 
        "web.app", "page.link",
        // Other major cloud providers
        "herokuapp.com", "herokudns.com", "vercel.app", "now.sh", 
        "netlify.app", "netlify.com", "github.io", "gitlab.io", "pages.dev",
        "digitalocean.com", "digitaloceanspaces.com", "linode.com", 
        "oraclecloud.com", "wpengine.com", "pantheonsite.io"
    ];
    
    CLOUD_HOSTS.iter().any(|&cloud_host| host.ends_with(cloud_host))
}

/// Enhanced generic platform domain detection
fn is_generic_platform_domain(host: &str) -> bool {
    const PLATFORM_HOSTS: &[&str] = &[
        "wordpress.com", "blogspot.com", "blogger.com", "wix.com", "wixsite.com", 
        "squarespace.com", "weebly.com", "jimdo.com", "site123.com", 
        "webflow.io", "godaddysites.com", "mystrikingly.com", "yolasite.com", 
        "tumblr.com", "carrd.co", "sites.google.com", "github.io"
    ];
    
    PLATFORM_HOSTS.iter().any(|&platform_host| host.ends_with(platform_host))
}

/// Enhanced social domain detection
fn is_social_domain(host: &str) -> bool {
    const SOCIAL_KEYWORDS_IN_HOST: &[&str] = &[
        "facebook", "twitter", "instagram", "linkedin", "youtube", 
        "pinterest", "tiktok", "snapchat", "reddit", "telegram", "discord"
    ];
    
    SOCIAL_KEYWORDS_IN_HOST.iter().any(|&keyword| host.contains(keyword))
}

/// Enhanced IP address detection
fn is_ip_address(domain_str: &str) -> bool {
    // IPv4 check
    if domain_str.split('.').count() == 4 && 
       domain_str.split('.').all(|part| part.parse::<u8>().is_ok()) {
        return true;
    }
    
    // IPv6 check (basic)
    if domain_str.contains(':') && !domain_str.contains("]:") && 
       domain_str.matches(':').count() > 1 && domain_str.matches(':').count() <= 7 {
        return true;
    }
    
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_enhanced_url_normalization() {
        let url = normalize_service_url("https://example.com/services/food?category=pantry&location=north#hours").unwrap();
        assert_eq!(url.domain, "example.com");
        assert_eq!(url.path_slugs, vec!["services", "food"]);
        assert!(url.significant_query_params.as_ref().unwrap().contains_key("category"));
        assert_eq!(url.fragment, Some("hours".to_string()));
    }

    #[test]
    fn test_query_param_matching() {
        let mut params1 = HashMap::new();
        params1.insert("category".to_string(), "food".to_string());
        params1.insert("location".to_string(), "north".to_string());
        
        let mut params2 = HashMap::new();
        params2.insert("category".to_string(), "nutrition".to_string()); // Related
        params2.insert("location".to_string(), "north".to_string()); // Exact match
        
        let adjustment = calculate_query_param_adjustment(&params1, &params2);
        assert!(adjustment > 0.0); // Should be positive due to location match and related category
    }

    #[test]
    fn test_weighted_slug_matching() {
        let slugs1 = vec!["services".to_string(), "food".to_string(), "pantry".to_string()];
        let slugs2 = vec!["services".to_string(), "food".to_string(), "different".to_string()];
        
        let score = count_weighted_matching_slugs(&slugs1, &slugs2);
        assert_eq!(score, FIRST_SLUG_WEIGHT + SECOND_SLUG_WEIGHT); // First two match
    }

    #[test]
    fn test_enhanced_slug_validation() {
        assert!(is_valid_slug_enhanced("services"));
        assert!(is_valid_slug_enhanced("food-bank"));
        assert!(is_valid_slug_enhanced("route-66")); // Should keep meaningful numbers
        assert!(!is_valid_slug_enhanced("index"));
        assert!(!is_valid_slug_enhanced("12345")); // Long ID
        assert!(!is_valid_slug_enhanced("page.html"));
    }

    #[test]
    fn test_fragment_relationship() {
        assert!(are_related_fragments("contact", "hours"));
        assert!(are_related_fragments("services", "programs"));
        assert!(!are_related_fragments("about", "login"));
    }
}