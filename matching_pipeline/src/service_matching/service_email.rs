// src/service_matching/service_email.rs
// Enhanced email matching with domain trustworthiness and refined generic detection

use anyhow::{Context, Result};
use chrono::Utc;
use log::{debug, info, warn};
use std::collections::{HashMap, HashSet};
use std::time::Instant;
use uuid::Uuid;

use crate::db::{self, PgPool};
use crate::models::{
    EmailMatchValue, MatchMethodType, MatchValues,
    ServiceId,
};
use serde_json;

use super::types::{ServiceEmailMatch, ServiceEmailMatchingStats, ServiceInfo};

// Enhanced configuration constants for service email matching
const BASE_EMAIL_CONFIDENCE: f64 = 0.95;
const GENERIC_EMAIL_PENALTY: f64 = 0.15;
const HIGH_FREQUENCY_PENALTY: f64 = 0.20;
const MODERATE_FREQUENCY_PENALTY: f64 = 0.10;
const HIGH_FREQUENCY_THRESHOLD: usize = 10;
const MODERATE_FREQUENCY_THRESHOLD: usize = 5;

// Enhanced thresholds
const HUGE_EMAIL_GROUP_THRESHOLD: usize = 500;
const HUGE_GROUP_CONFIDENCE_PENALTY: f64 = 0.50;

// NEW: Domain trustworthiness constants
const ORGANIZATIONAL_DOMAIN_BONUS: f64 = 0.08; // Bonus for organizational domains
const FREE_PROVIDER_PENALTY: f64 = 0.12; // Penalty for free email providers
const VERY_LOW_CONFIDENCE_THRESHOLD: f64 = 0.15; // Threshold for discarding very weak matches

// NEW: Role-based email penalties (refined generic detection)
const HIGHLY_GENERIC_PENALTY: f64 = 0.20; // For info@, contact@, etc.
const ROLE_BASED_PENALTY: f64 = 0.08; // For admin@, support@, etc.
const DEPARTMENT_SPECIFIC_PENALTY: f64 = 0.04; // For dept-specific emails on specific domains

/// Enhanced entry point for service email matching within a cluster
pub async fn find_service_email_matches(
    pool: &PgPool,
    services: &[ServiceInfo],
    pipeline_run_id: &str,
    cluster_id: Option<&str>,
) -> Result<(Vec<ServiceEmailMatch>, ServiceEmailMatchingStats)> {
    let start_time = Instant::now();
    let mut stats = ServiceEmailMatchingStats::default();

    debug!(
        "Starting enhanced service email matching for cluster {:?} with {} services",
        cluster_id.unwrap_or("unknown"),
        services.len()
    );

    if services.len() < 2 {
        info!("Insufficient services for email matching (need at least 2) in cluster {:?}.", cluster_id.unwrap_or("unknown"));
        stats.processing_time = start_time.elapsed();
        return Ok((Vec::new(), stats));
    }

    // Step 1: Build email frequency map and domain analysis
    let email_frequency = build_email_frequency_map(services);
    let domain_trust_scores = analyze_domain_trustworthiness(services);
    
    debug!("Built email frequency map with {} unique normalized emails for cluster {:?}", 
           email_frequency.len(), cluster_id.unwrap_or("unknown"));
    debug!("Analyzed {} unique email domains for trustworthiness", domain_trust_scores.len());

    // Step 2: Group services by normalized email
    let email_groups = group_services_by_email(services);
    debug!("Grouped services into {} email groups for cluster {:?}", 
           email_groups.len(), cluster_id.unwrap_or("unknown"));

    let mut matches = Vec::new();
    let mut confidence_scores = Vec::new();

    for (normalized_email, service_group) in email_groups {
        if service_group.len() < 2 {
            continue;
        }

        stats.services_processed += service_group.len();

        // Enhanced confidence calculation
        let email_analysis = analyze_email_characteristics(&normalized_email, &domain_trust_scores);
        let frequency = *email_frequency.get(&normalized_email).unwrap_or(&0);
        
        // Calculate all adjustments
        let frequency_adjustment = calculate_frequency_adjustment(frequency);
        let generic_adjustment = email_analysis.generic_penalty;
        let domain_adjustment = email_analysis.domain_trust_adjustment;
        
        // Check for huge problematic groups
        let is_huge_group = service_group.len() > HUGE_EMAIL_GROUP_THRESHOLD;
        let huge_group_penalty = if is_huge_group && 
            (email_analysis.is_highly_generic || frequency >= MODERATE_FREQUENCY_THRESHOLD) {
            warn!("Applying HUGE_GROUP_CONFIDENCE_PENALTY to email group '{}' ({} services, highly_generic: {}, freq: {}) in cluster {:?}",
                  normalized_email, service_group.len(), email_analysis.is_highly_generic, frequency, cluster_id.unwrap_or("unknown"));
            HUGE_GROUP_CONFIDENCE_PENALTY
        } else {
            0.0
        };

        // Generate all pairs within this email group
        for i in 0..service_group.len() {
            for j in (i + 1)..service_group.len() {
                stats.pairs_compared += 1;

                let service1 = &service_group[i];
                let service2 = &service_group[j];

                // Calculate final confidence score with all adjustments
                let base_confidence = BASE_EMAIL_CONFIDENCE;
                let final_confidence = (base_confidence 
                    - frequency_adjustment 
                    - generic_adjustment 
                    - huge_group_penalty
                    + domain_adjustment) // Domain adjustment can be positive or negative
                    .max(0.05) // Ensure minimum confidence
                    .min(1.0); // Ensure maximum confidence

                // Discard very weak matches from problematic email groups
                if final_confidence < VERY_LOW_CONFIDENCE_THRESHOLD {
                    debug!("Discarding very low confidence ({:.3}) email match for {} <-> {} due to penalties.",
                           final_confidence, service1.service_id.0, service2.service_id.0);
                    continue;
                }

                let service_match = ServiceEmailMatch {
                    service_id_1: service1.service_id.clone(),
                    service_id_2: service2.service_id.clone(),
                    confidence_score: final_confidence,
                    match_values: MatchValues::ServiceEmail(EmailMatchValue {
                        original_email1: service1.email.as_ref().unwrap_or(&String::new()).clone(),
                        original_email2: service2.email.as_ref().unwrap_or(&String::new()).clone(),
                        normalized_shared_email: normalized_email.clone(),
                    }),
                    frequency_adjustment,
                    generic_email_detected: email_analysis.is_highly_generic || email_analysis.is_role_based,
                };

                matches.push(service_match);
                confidence_scores.push(final_confidence);
                stats.matches_found += 1;

                // Update stats categories
                if email_analysis.is_highly_generic || email_analysis.is_role_based {
                    stats.generic_email_matches += 1;
                }
                if frequency >= HIGH_FREQUENCY_THRESHOLD {
                    stats.high_frequency_matches += 1;
                }

                debug!(
                    "Enhanced email match: {} <-> {} (confidence: {:.3}, email: '{}', generic: {}, freq: {}, domain_trust: {:.2}, huge_penalty: {:.2})",
                    service1.service_id.0,
                    service2.service_id.0,
                    final_confidence,
                    normalized_email,
                    email_analysis.is_highly_generic,
                    frequency,
                    email_analysis.domain_trust_adjustment,
                    huge_group_penalty
                );
            }
        }
    }

    // Calculate final statistics
    stats.avg_confidence = if !confidence_scores.is_empty() {
        confidence_scores.iter().sum::<f64>() / confidence_scores.len() as f64
    } else {
        0.0
    };
    stats.processing_time = start_time.elapsed();

    info!(
        "Enhanced email matching completed for cluster {:?}: {} matches from {} pairs in {:.2?}",
        cluster_id.unwrap_or("unknown"),
        stats.matches_found,
        stats.pairs_compared,
        stats.processing_time
    );
    info!(
        "Enhanced match breakdown for cluster {:?} - Generic emails: {}, High frequency: {}, Avg confidence: {:.3}",
        cluster_id.unwrap_or("unknown"),
        stats.generic_email_matches,
        stats.high_frequency_matches,
        stats.avg_confidence
    );

    Ok((matches, stats))
}

/// NEW: Analyze domain trustworthiness for confidence adjustments
fn analyze_domain_trustworthiness(services: &[ServiceInfo]) -> HashMap<String, DomainTrustScore> {
    let mut domain_analysis = HashMap::new();
    
    // Collect all email domains
    let mut domain_counts = HashMap::new();
    for service in services {
        if let Some(email) = &service.email {
            if let Some(domain) = extract_email_domain(email) {
                *domain_counts.entry(domain).or_insert(0) += 1;
            }
        }
    }
    
    // Analyze each domain
    for (domain, count) in domain_counts {
        let trust_score = calculate_domain_trust_score(&domain, count);
        domain_analysis.insert(domain, trust_score);
    }
    
    domain_analysis
}

#[derive(Debug, Clone)]
struct DomainTrustScore {
    is_organizational: bool,
    is_free_provider: bool,
    is_governmental: bool,
    is_educational: bool,
    is_healthcare: bool,
    specificity_score: f64, // 0.0 to 1.0, higher = more specific
    trust_adjustment: f64, // Positive = bonus, negative = penalty
}

/// NEW: Calculate trust score for an email domain
fn calculate_domain_trust_score(domain: &str, usage_count: usize) -> DomainTrustScore {
    let domain_lower = domain.to_lowercase();
    
    // Check domain type
    let is_governmental = domain_lower.ends_with(".gov") || 
                         domain_lower.ends_with(".mil") ||
                         domain_lower.contains(".gov.") ||
                         domain_lower.contains("city.") ||
                         domain_lower.contains("county.");
    
    let is_educational = domain_lower.ends_with(".edu") || 
                        domain_lower.contains(".edu.") ||
                        domain_lower.contains("university") ||
                        domain_lower.contains("college") ||
                        domain_lower.contains("school");
    
    let is_healthcare = domain_lower.contains("health") ||
                       domain_lower.contains("hospital") ||
                       domain_lower.contains("clinic") ||
                       domain_lower.contains("medical");
    
    let is_free_provider = is_free_email_provider(&domain_lower);
    
    let is_organizational = (domain_lower.ends_with(".org") || 
                           domain_lower.ends_with(".net") ||
                           is_governmental || 
                           is_educational || 
                           is_healthcare) && 
                           !is_free_provider;
    
    // Calculate specificity (longer, more specific domains are generally more trustworthy)
    let specificity_score = calculate_domain_specificity(&domain_lower, usage_count);
    
    // Calculate trust adjustment
    let mut trust_adjustment = 0.0;
    
    if is_governmental {
        trust_adjustment += ORGANIZATIONAL_DOMAIN_BONUS * 1.5; // Extra bonus for gov
    } else if is_educational || is_healthcare {
        trust_adjustment += ORGANIZATIONAL_DOMAIN_BONUS * 1.2; // Strong bonus for edu/health
    } else if is_organizational {
        trust_adjustment += ORGANIZATIONAL_DOMAIN_BONUS; // Standard organizational bonus
    }
    
    if is_free_provider {
        trust_adjustment -= FREE_PROVIDER_PENALTY;
    }
    
    // Apply specificity bonus/penalty
    if specificity_score > 0.7 {
        trust_adjustment += 0.03; // Bonus for highly specific domains
    } else if specificity_score < 0.3 {
        trust_adjustment -= 0.02; // Small penalty for very generic domains
    }
    
    DomainTrustScore {
        is_organizational,
        is_free_provider,
        is_governmental,
        is_educational,
        is_healthcare,
        specificity_score,
        trust_adjustment,
    }
}

/// NEW: Calculate domain specificity score
fn calculate_domain_specificity(domain: &str, usage_count: usize) -> f64 {
    let mut score = 0.5; // Base score
    
    // Longer domains are generally more specific
    let length_score = (domain.len() as f64 / 50.0).min(1.0);
    score += length_score * 0.2;
    
    // Multiple subdomains suggest organizational structure
    let subdomain_count = domain.matches('.').count();
    if subdomain_count >= 3 {
        score += 0.2;
    } else if subdomain_count >= 2 {
        score += 0.1;
    }
    
    // Contains organization-like keywords
    let org_keywords = ["foundation", "center", "centre", "institute", "association", 
                       "society", "council", "coalition", "alliance", "network"];
    if org_keywords.iter().any(|&keyword| domain.contains(keyword)) {
        score += 0.2;
    }
    
    // Service-specific keywords
    let service_keywords = ["food", "health", "legal", "housing", "employment", 
                           "mental", "crisis", "emergency", "community"];
    if service_keywords.iter().any(|&keyword| domain.contains(keyword)) {
        score += 0.1;
    }
    
    // Geographic indicators (can be good or bad depending on context)
    let geo_keywords = ["city", "county", "state", "regional", "local"];
    if geo_keywords.iter().any(|&keyword| domain.contains(keyword)) {
        score += 0.1;
    }
    
    // Penalty for very high usage (might indicate a platform rather than specific org)
    if usage_count > 20 {
        score -= 0.3;
    } else if usage_count > 10 {
        score -= 0.1;
    }
    
    score.max(0.0).min(1.0)
}

/// NEW: Enhanced free email provider detection
fn is_free_email_provider(domain: &str) -> bool {
    const FREE_PROVIDERS: &[&str] = &[
        // Major providers
        "gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "live.com",
        "icloud.com", "me.com", "mac.com", "aol.com", "msn.com",
        
        // International providers
        "yandex.com", "yandex.ru", "mail.ru", "163.com", "126.com", "qq.com",
        "sina.com", "sohu.com", "yeah.net", "foxmail.com",
        
        // Other common free providers
        "protonmail.com", "tutanota.com", "fastmail.com", "zoho.com",
        "gmx.com", "web.de", "t-online.de", "orange.fr", "laposte.net",
        "free.fr", "sfr.fr", "wanadoo.fr", "alice.it", "libero.it",
        "virgilio.it", "tiscali.it", "terra.com", "bol.com.br",
        
        // Temporary/disposable providers
        "tempmail.com", "guerrillamail.com", "10minutemail.com",
        "mailinator.com", "throwaway.email",
    ];
    
    FREE_PROVIDERS.contains(&domain) || 
    // Check for common free provider patterns
    domain.contains("temp") || 
    domain.contains("disposable") ||
    domain.contains("throwaway")
}

/// NEW: Extract domain from email address
fn extract_email_domain(email: &str) -> Option<String> {
    if let Some(at_pos) = email.rfind('@') {
        let domain = &email[at_pos + 1..];
        if !domain.is_empty() && domain.contains('.') {
            Some(domain.to_lowercase())
        } else {
            None
        }
    } else {
        None
    }
}

#[derive(Debug)]
struct EmailAnalysis {
    is_highly_generic: bool,
    is_role_based: bool,
    is_department_specific: bool,
    generic_penalty: f64,
    domain_trust_adjustment: f64,
}

/// NEW: Comprehensive email characteristic analysis
fn analyze_email_characteristics(
    normalized_email: &str, 
    domain_trust_scores: &HashMap<String, DomainTrustScore>
) -> EmailAnalysis {
    let is_highly_generic = is_highly_generic_email(normalized_email);
    let is_role_based = is_role_based_email(normalized_email);
    let is_department_specific = is_department_specific_email(normalized_email);
    
    // Calculate generic penalty
    let generic_penalty = if is_highly_generic {
        HIGHLY_GENERIC_PENALTY
    } else if is_role_based {
        ROLE_BASED_PENALTY
    } else if is_department_specific {
        DEPARTMENT_SPECIFIC_PENALTY
    } else {
        0.0
    };
    
    // Get domain trust adjustment
    let domain_trust_adjustment = if let Some(domain) = extract_email_domain(normalized_email) {
        domain_trust_scores.get(&domain)
            .map(|score| score.trust_adjustment)
            .unwrap_or(0.0)
    } else {
        0.0
    };
    
    EmailAnalysis {
        is_highly_generic,
        is_role_based,
        is_department_specific,
        generic_penalty,
        domain_trust_adjustment,
    }
}

/// NEW: Enhanced highly generic email detection
fn is_highly_generic_email(email: &str) -> bool {
    const HIGHLY_GENERIC_PREFIXES: &[&str] = &[
        "info@", "contact@", "hello@", "hi@", "mail@", "email@",
        "no-reply@", "noreply@", "donotreply@", "do-not-reply@",
        "auto@", "automated@", "system@", "postmaster@", "webmaster@",
        "general@", "main@", "office@", "headquarters@", "hq@",
    ];
    
    HIGHLY_GENERIC_PREFIXES.iter().any(|prefix| email.starts_with(prefix))
}

/// NEW: Role-based email detection (less generic than highly generic, but still organizational)
fn is_role_based_email(email: &str) -> bool {
    const ROLE_BASED_PREFIXES: &[&str] = &[
        "admin@", "administrator@", "support@", "help@", "service@",
        "customer@", "client@", "inquiry@", "inquiries@", "sales@",
        "marketing@", "communications@", "media@", "press@", "news@",
        "hr@", "humanresources@", "finance@", "accounting@", "billing@",
        "legal@", "compliance@", "security@", "it@", "tech@",
        "development@", "research@", "volunteer@", "volunteers@",
        "donations@", "giving@", "fundraising@", "grants@",
        "operations@", "management@", "director@", "executive@",
    ];
    
    ROLE_BASED_PREFIXES.iter().any(|prefix| email.starts_with(prefix))
}

/// NEW: Department-specific email detection (more specific than role-based)
fn is_department_specific_email(email: &str) -> bool {
    const DEPARTMENT_PREFIXES: &[&str] = &[
        // Service-specific departments
        "foodbank@", "foodpantry@", "meals@", "nutrition@",
        "housing@", "shelter@", "homeless@", "residence@",
        "health@", "medical@", "clinic@", "dental@", "mental@",
        "counseling@", "therapy@", "behavioral@", "wellness@",
        "legal@", "law@", "attorney@", "advocate@", "immigration@",
        "employment@", "jobs@", "career@", "workforce@", "training@",
        "childcare@", "daycare@", "youth@", "children@", "family@",
        "senior@", "seniors@", "elderly@", "aging@", "adult@",
        "emergency@", "crisis@", "hotline@", "urgent@",
        "transportation@", "transport@", "mobility@",
        "benefits@", "assistance@", "aid@", "relief@", "welfare@",
        
        // Common department names
        "intake@", "admission@", "admissions@", "enrollment@",
        "outreach@", "education@", "prevention@", "advocacy@",
        "referral@", "referrals@", "coordination@", "case@",
    ];
    
    DEPARTMENT_PREFIXES.iter().any(|prefix| email.starts_with(prefix))
}

/// Build frequency map for all normalized emails in the service set
fn build_email_frequency_map(services: &[ServiceInfo]) -> HashMap<String, usize> {
    let mut frequency_map = HashMap::new();
    for service in services {
        if let Some(email) = &service.email {
            let normalized = normalize_email(email);
            if !normalized.is_empty() {
                *frequency_map.entry(normalized).or_insert(0) += 1;
            }
        }
    }
    frequency_map
}

/// Group services by their normalized email addresses
fn group_services_by_email(services: &[ServiceInfo]) -> HashMap<String, Vec<&ServiceInfo>> {
    let mut email_groups = HashMap::new();
    for service in services {
        if let Some(email) = &service.email {
            let normalized = normalize_email(email);
            if !normalized.is_empty() {
                email_groups
                    .entry(normalized)
                    .or_insert_with(Vec::new)
                    .push(service);
            }
        }
    }
    email_groups
}

/// Calculate confidence adjustment based on email frequency
fn calculate_frequency_adjustment(frequency: usize) -> f64 {
    if frequency >= HIGH_FREQUENCY_THRESHOLD {
        HIGH_FREQUENCY_PENALTY
    } else if frequency >= MODERATE_FREQUENCY_THRESHOLD {
        MODERATE_FREQUENCY_PENALTY
    } else {
        0.0
    }
}

/// Enhanced email normalization function
pub fn normalize_email(email: &str) -> String {
    let email_trimmed = email.trim().to_lowercase();
    if !email_trimmed.contains('@') {
        return String::new();
    }

    let parts: Vec<&str> = email_trimmed.splitn(2, '@').collect();
    if parts.len() != 2 || parts[0].is_empty() || parts[1].is_empty() {
        return String::new();
    }

    let (local_part_full, domain_part) = (parts[0], parts[1]);

    // Remove part after '+' (email aliasing)
    let local_part_no_plus = local_part_full.split('+').next().unwrap_or("").to_string();

    // Normalize domain (e.g., googlemail.com -> gmail.com)
    let final_domain_part = match domain_part {
        "googlemail.com" => "gmail.com",
        "outlook.com" => "outlook.com", // Keep as is, but could normalize other variations
        "hotmail.com" => "outlook.com", // Normalize to current Microsoft domain
        "live.com" => "outlook.com",    // Normalize to current Microsoft domain
        "msn.com" => "outlook.com",     // Normalize to current Microsoft domain
        _ => domain_part,
    };

    // Remove dots from local part for Gmail addresses (and similar providers)
    let final_local_part = if matches!(final_domain_part, "gmail.com" | "googlemail.com") {
        local_part_no_plus.replace('.', "")
    } else {
        local_part_no_plus
    };

    if final_local_part.is_empty() {
        String::new()
    } else {
        format!("{}@{}", final_local_part, final_domain_part)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EntityId;

    #[test]
    fn test_enhanced_email_normalization() {
        assert_eq!(normalize_email("user@example.com"), "user@example.com");
        assert_eq!(normalize_email("USER@EXAMPLE.COM"), "user@example.com");
        assert_eq!(normalize_email("user+tag@example.com"), "user@example.com");
        assert_eq!(normalize_email("user.name@gmail.com"), "username@gmail.com");
        assert_eq!(normalize_email("user@hotmail.com"), "user@outlook.com"); // Domain normalization
        assert_eq!(normalize_email("user@live.com"), "user@outlook.com");
        assert_eq!(normalize_email("  user@example.com  "), "user@example.com");
        assert_eq!(normalize_email("invalid-email"), "");
        assert_eq!(normalize_email("@example.com"), "");
        assert_eq!(normalize_email("user@"), "");
    }

    #[test]
    fn test_enhanced_generic_email_detection() {
        assert!(is_highly_generic_email("info@example.com"));
        assert!(is_highly_generic_email("no-reply@company.org"));
        assert!(!is_highly_generic_email("john.doe@company.com"));
        
        assert!(is_role_based_email("admin@example.com"));
        assert!(is_role_based_email("support@company.org"));
        assert!(!is_role_based_email("info@company.com")); // This is highly generic, not role-based
        
        assert!(is_department_specific_email("foodbank@charity.org"));
        assert!(is_department_specific_email("housing@services.gov"));
        assert!(!is_department_specific_email("admin@company.com"));
    }

    #[test]
    fn test_domain_trust_analysis() {
        assert!(is_free_email_provider("gmail.com"));
        assert!(is_free_email_provider("yahoo.com"));
        assert!(!is_free_email_provider("redcross.org"));
        assert!(!is_free_email_provider("seattle.gov"));
        
        let domain_score = calculate_domain_trust_score("foodbank.org", 2);
        assert!(domain_score.is_organizational);
        assert!(!domain_score.is_free_provider);
        assert!(domain_score.trust_adjustment > 0.0);
        
        let free_domain_score = calculate_domain_trust_score("gmail.com", 50);
        assert!(!free_domain_score.is_organizational);
        assert!(free_domain_score.is_free_provider);
        assert!(free_domain_score.trust_adjustment < 0.0);
    }

    #[test]
    fn test_domain_specificity() {
        let specific_score = calculate_domain_specificity("seattle.foodbank.org", 2);
        let generic_score = calculate_domain_specificity("example.com", 25);
        
        assert!(specific_score > generic_score);
        assert!(specific_score > 0.5);
    }

    #[test]
    fn test_email_characteristic_analysis() {
        let mut domain_scores = HashMap::new();
        domain_scores.insert("foodbank.org".to_string(), DomainTrustScore {
            is_organizational: true,
            is_free_provider: false,
            is_governmental: false,
            is_educational: false,
            is_healthcare: false,
            specificity_score: 0.8,
            trust_adjustment: 0.08,
        });
        
        let analysis = analyze_email_characteristics("info@foodbank.org", &domain_scores);
        assert!(analysis.is_highly_generic);
        assert!(!analysis.is_role_based);
        assert!(analysis.generic_penalty > 0.0);
        assert!(analysis.domain_trust_adjustment > 0.0);
        
        let dept_analysis = analyze_email_characteristics("intake@foodbank.org", &domain_scores);
        assert!(!dept_analysis.is_highly_generic);
        assert!(!dept_analysis.is_role_based);
        assert!(dept_analysis.is_department_specific);
        assert!(dept_analysis.generic_penalty < analysis.generic_penalty);
    }
}