// src/service_matching/service_name.rs
// Enhanced service name matching strategies with advanced location handling and contextual rules

use regex::Regex;
use std::collections::{HashMap, HashSet};
use strsim::jaro_winkler;

use crate::{models::{MatchValues, ServiceNameMatchValue}, MatchMethodType};

use super::types::{ServiceInfo, ServiceMatch};

// Enhanced configuration constants
const SERVICE_NAME_FUZZY_THRESHOLD: f64 = 0.88;
const SERVICE_NAME_COMBINED_THRESHOLD: f64 = 0.85;
const FUZZY_WEIGHT: f64 = 0.4;
const SEMANTIC_WEIGHT: f64 = 0.6;
const MIN_TOKEN_LENGTH: usize = 2;
const MIN_TOKEN_OVERLAP: usize = 2;

// NEW: Location handling constants
const LOCATION_WEIGHT: f64 = 0.15; // Weight for location similarity in overall score
const LOCATION_CONFLICT_PENALTY: f64 = 0.25; // Penalty for conflicting specific locations
const LOCATION_MATCH_BONUS: f64 = 0.08; // Bonus for matching specific locations

// Enhanced service-specific stopwords
const SERVICE_STOPWORDS: [&str; 128] = [
    // Basic articles and conjunctions
    "a", "an", "the", "and", "or", "but", "nor", "for", "yet", "so", "in", "on", "at", "by",
    "to", "with", "from", "of", "as", "into", "about", "before", "after", "during", "until",
    "since", "unless",
    // Service-related terms
    "service", "services", "program", "programs", "support", "help", "assistance", "aid", 
    "care", "center", "centre", "clinic", "facility", "location", "office", "department",
    "division", "unit", "bureau", "agency", "organization", "foundation", "institute",
    "association", "society", "council", "committee",
    // Geographic and organizational
    "community", "local", "regional", "county", "state", "federal", "public", "private",
    "nonprofit", "non-profit", "charitable", "volunteer", "outreach", "resource", "resources",
    "network", "coalition", "alliance", "group", "team", "staff", "professional",
    // Service delivery methods
    "emergency", "crisis", "hotline", "helpline", "referral", "information", "guide",
    "directory", "database", "system", "portal", "website", "online", "virtual",
    "mobile", "telephone", "phone", "call", "contact", "connect", "access",
    // Additional common terms
    "main", "central", "primary", "secondary", "branch", "satellite", "outpost",
    "headquarters", "hq", "administrative", "admin", "general", "comprehensive",
    "specialized", "dedicated", "focused", "based", "oriented", "driven",
    // Time and frequency indicators
    "24/7", "24hr", "24-hour", "daily", "weekly", "monthly", "annual", "ongoing",
    "temporary", "permanent", "seasonal", "year-round"
];

// Enhanced service type indicators with better coverage
const SERVICE_TYPE_INDICATORS: [(&str, &str); 72] = [
    // Food assistance (expanded)
    ("food bank", "food_assistance"), ("food pantry", "food_assistance"),
    ("soup kitchen", "food_assistance"), ("meal", "food_assistance"),
    ("nutrition", "food_assistance"), ("feeding", "food_assistance"),
    ("hunger", "food_assistance"), ("grocery", "food_assistance"),
    ("snap", "food_assistance"), ("wic", "food_assistance"),
    ("food stamps", "food_assistance"), ("food distribution", "food_assistance"),
    
    // Housing (expanded)
    ("shelter", "housing"), ("housing", "housing"), ("homeless", "housing"),
    ("transitional", "housing"), ("affordable housing", "housing"),
    ("emergency housing", "housing"), ("temporary housing", "housing"),
    ("residential", "housing"), ("accommodation", "housing"),
    
    // Mental health (expanded)
    ("mental health", "mental_health"), ("counseling", "mental_health"),
    ("therapy", "mental_health"), ("psychiatric", "mental_health"),
    ("behavioral health", "mental_health"), ("psychological", "mental_health"),
    ("wellness", "mental_health"), ("emotional support", "mental_health"),
    
    // Substance abuse (expanded)
    ("substance abuse", "substance_abuse"), ("addiction", "substance_abuse"),
    ("recovery", "substance_abuse"), ("detox", "substance_abuse"),
    ("rehabilitation", "substance_abuse"), ("rehab", "substance_abuse"),
    ("alcohol", "substance_abuse"), ("drug", "substance_abuse"),
    
    // Healthcare (expanded)
    ("medical", "healthcare"), ("health", "healthcare"), ("clinic", "healthcare"),
    ("hospital", "healthcare"), ("dental", "healthcare"), ("pharmacy", "healthcare"),
    ("doctor", "healthcare"), ("physician", "healthcare"), ("nurse", "healthcare"),
    ("primary care", "healthcare"), ("urgent care", "healthcare"),
    
    // Legal (expanded)
    ("legal aid", "legal"), ("legal services", "legal"), ("legal", "legal"),
    ("law", "legal"), ("attorney", "legal"), ("immigration", "legal"),
    ("court", "legal"), ("justice", "legal"), ("advocacy", "legal"),
    
    // Employment (expanded)
    ("employment", "employment"), ("job", "employment"), ("career", "employment"),
    ("training", "employment"), ("workforce", "employment"), ("vocational", "employment"),
    ("skills", "employment"), ("placement", "employment"),
    
    // Childcare and family
    ("childcare", "childcare"), ("daycare", "childcare"), ("child care", "childcare"),
    ("youth", "youth_services"), ("family", "family_services"),
    ("parenting", "family_services"), ("children", "family_services"),
];

// Enhanced incompatible service types
const INCOMPATIBLE_SERVICE_TYPES: [(&str, &str); 12] = [
    ("food_assistance", "legal"), ("food_assistance", "childcare"),
    ("housing", "employment"), ("housing", "food_assistance"),
    ("mental_health", "food_assistance"), ("mental_health", "employment"),
    ("substance_abuse", "childcare"), ("substance_abuse", "food_assistance"),
    ("healthcare", "legal"), ("healthcare", "employment"),
    ("employment", "childcare"), ("youth_services", "substance_abuse"),
];

// Enhanced service abbreviations
const SERVICE_ABBREVIATIONS: [(&str, &str); 36] = [
    // Common service abbreviations
    (r"\bfb\b", "food bank"), (r"\bfp\b", "food pantry"),
    (r"\bmh\b", "mental health"), (r"\bsa\b", "substance abuse"),
    (r"\bla\b", "legal aid"), (r"\bcc\b", "community center"),
    (r"\bhc\b", "health center"), (r"\bdc\b", "day care"),
    (r"\bec\b", "emergency care"), (r"\bfs\b", "family services"),
    (r"\bcs\b", "community services"), (r"\bys\b", "youth services"),
    (r"\bss\b", "social services"), (r"\bvs\b", "veteran services"),
    
    // Government programs
    (r"\bebt\b", "electronic benefits transfer"), (r"\bwic\b", "women infants children"),
    (r"\bsnap\b", "supplemental nutrition assistance program"),
    (r"\btanf\b", "temporary assistance needy families"),
    (r"\bliheap\b", "low income home energy assistance program"),
    (r"\bmedicaid\b", "medicaid"), (r"\bmedicare\b", "medicare"),
    (r"\bssi\b", "supplemental security income"), (r"\bssdi\b", "social security disability insurance"),
    
    // Information and referral
    (r"\b211\b", "two one one information referral"), (r"\b911\b", "nine one one emergency"),
    (r"\b311\b", "three one one city services"), (r"\b411\b", "four one one directory assistance"),
    
    // Common organizational abbreviations
    (r"\bngo\b", "non governmental organization"), (r"\bcbo\b", "community based organization"),
    (r"\bfqhc\b", "federally qualified health center"), (r"\bcmhc\b", "community mental health center"),
    (r"\bdhhs\b", "department health human services"), (r"\bdhs\b", "department human services"),
    (r"\bdcf\b", "department children families"), (r"\bdss\b", "department social services"),
    (r"\bva\b", "veterans affairs"),
];

// NEW: Enhanced location indicators with regional specificity
const LOCATION_INDICATORS: [&str; 51] = [
    // Directional
    "north", "south", "east", "west", "northeast", "northwest", "southeast", "southwest",
    "northern", "southern", "eastern", "western", "central", "downtown", "uptown", "midtown",
    
    // Urban areas
    "city", "urban", "metro", "metropolitan", "district", "neighborhood", "suburb", "suburban",
    "downtown", "inner city", "outer", "greater",
    
    // Administrative
    "county", "parish", "borough", "township", "municipality", "region", "regional",
    "state", "federal", "provincial", "local",
    
    // Geographic features
    "valley", "hill", "mountain", "river", "lake", "bay", "coast", "coastal", "inland",
    "island", "peninsula", "plateau"
];

// NEW: Numeric/code pattern handlers
const NUMERIC_PATTERNS: [(&str, &str); 8] = [
    (r"\broute\s+(\d+)", "route $1"), // Route 66 -> route 66
    (r"\bhighway\s+(\d+)", "highway $1"), // Highway 101 -> highway 101
    (r"\bprogram\s+([a-z])-?(\d+)", "program $1 $2"), // Program A-123 -> program a 123
    (r"\bunit\s+(\d+[a-z]?)", "unit $1"), // Unit 5A -> unit 5a
    (r"\bsite\s+(\d+)", "site $1"), // Site 3 -> site 3
    (r"\bbuilding\s+(\d+[a-z]?)", "building $1"), // Building 2B -> building 2b
    (r"\broom\s+(\d+[a-z]?)", "room $1"), // Room 105A -> room 105a
    (r"\bfloor\s+(\d+)", "floor $1"), // Floor 3 -> floor 3
];

/// Enhanced service name normalization with comprehensive multi-phase processing
pub fn normalize_service_name_advanced(name: &str) -> (String, Option<&'static str>) {
    let mut normalized = name.to_lowercase().trim().to_string();

    // Phase 1: Remove noise and clean up common artifacts
    normalized = remove_service_noise(&normalized);

    // Phase 2: Handle numeric/code patterns before other normalization
    normalized = normalize_numeric_patterns(&normalized);

    // Phase 3: Detect service type from the less processed name for better accuracy
    let service_type = detect_service_type(&normalized);

    // Phase 4: Character substitutions and general cleanup
    normalized = apply_character_substitutions(&normalized);

    // Phase 5: Expand common service-related abbreviations
    normalized = expand_service_abbreviations(&normalized);

    // Phase 6: Remove common prefixes/suffixes that don't add semantic value for matching
    normalized = remove_service_affixes(&normalized);

    // Phase 7: Final cleanup of whitespace and non-essential characters
    normalized = final_service_cleanup(&normalized);

    (normalized, service_type)
}

// NEW: Handle numeric and code patterns specially
fn normalize_numeric_patterns(name: &str) -> String {
    let mut result = name.to_string();
    
    for (pattern, replacement) in &NUMERIC_PATTERNS {
        if let Ok(re) = Regex::new(pattern) {
            result = re.replace_all(&result, *replacement).to_string();
        }
    }
    
    result
}

fn remove_service_noise(name: &str) -> String {
    let mut result = name.to_string();
    
    // Enhanced noise patterns including more organizational artifacts
    let noise_patterns = [
        r"^\s*\*+\s*", r"\s*\*+\s*$", // Asterisks
        r"^\s*-+\s*", r"\s*-+\s*$", // Dashes
        r"^\s*\d+\.\s*", r"^\s*\d+\)\s*", // List numbers
        r"\s*\([^)]*\)\s*$", // Trailing parentheses
        r"^\s*\[[^\]]*\]\s*", r"\s*\[[^\]]*\]\s*$", // Brackets
        r"(?i)\s*\(formerly .*\)\s*$", // (formerly ...)
        r"(?i)\s*aka\s+.*$", // aka ...
        r"(?i)\s*\(est\.?\s*\d{4}\)\s*$", // (est. 1995)
        r"(?i)\s*since\s+\d{4}\s*$", // since 1995
        r"(?i)\s*\(non-?profit\)\s*$", // (nonprofit)
        r"(?i)\s*\(501c3\)\s*$", // (501c3)
        r"\s*[©®™]\s*", // Copyright symbols
        r"\s*\bllc\b\s*$", r"\s*\binc\b\s*$", r"\s*\bcorp\b\s*$", // Corporate suffixes
    ];
    
    for pattern in &noise_patterns {
        if let Ok(re) = Regex::new(pattern) {
            result = re.replace_all(&result, " ").trim().to_string();
        }
    }
    
    result
}

fn detect_service_type(normalized_name: &str) -> Option<&'static str> {
    // Sort indicators by length (longest first) for better matching
    let mut sorted_indicators = SERVICE_TYPE_INDICATORS.to_vec();
    sorted_indicators.sort_by(|a, b| b.0.len().cmp(&a.0.len()));
    
    for (indicator, service_type) in &sorted_indicators {
        if let Ok(re) = Regex::new(&format!(r"\b{}\b", regex::escape(indicator))) {
            if re.is_match(normalized_name) {
                return Some(service_type);
            }
        }
    }
    None
}

fn apply_character_substitutions(name: &str) -> String {
    let mut result = name.to_string();
    
    // Enhanced substitutions including more punctuation and special cases
    let substitutions = [
        ("&", " and "), ("+", " plus "), ("/", " "), ("-", " "),
        (".", " "), ("'", ""), ("(", " "), (")", " "), (",", " "),
        (":", " "), (";", " "), ("\"", ""), 
        // Unicode quote characters properly escaped
        ("\u{201C}", ""), ("\u{201D}", ""), // Left and right double quotes
        ("\u{2018}", ""), ("\u{2019}", ""), // Left and right single quotes (curly apostrophes)
        ("—", " "), ("–", " "), ("_", " "),
        ("#", " number "), ("@", " at "), ("%", " percent "),
    ];
    
    for (from, to) in &substitutions {
        result = result.replace(from, to);
    }
    
    result
}

fn expand_service_abbreviations(name: &str) -> String {
    let mut result = name.to_string();
    
    // Sort by length (longest first) to avoid partial replacements
    let mut sorted_abbreviations = SERVICE_ABBREVIATIONS.to_vec();
    sorted_abbreviations.sort_by(|a, b| b.0.len().cmp(&a.0.len()));
    
    for (abbrev_pattern, expansion) in &sorted_abbreviations {
        if let Ok(re) = Regex::new(abbrev_pattern) {
            result = re.replace_all(&result, *expansion).to_string();
        }
    }
    
    result
}

fn remove_service_affixes(name: &str) -> String {
    let mut result = name.to_string();
    
    // Enhanced prefixes including more organizational and temporal indicators
    let prefixes = [
        "the ", "a ", "an ", "our ", "your ", "my ", "new ", "old ", "free ",
        "24/7 ", "24hr ", "24 hour ", "emergency ", "crisis ", "immediate ", "urgent ",
        "official ", "authorized ", "certified ", "licensed ", "registered ",
        "greater ", "metro ", "regional ", "local ", "county ", "state ", "federal ",
        "non-profit ", "nonprofit ", "charitable ", "volunteer ",
    ];
    
    for prefix in &prefixes {
        if result.starts_with(prefix) {
            result = result[prefix.len()..].trim().to_string();
        }
    }
    
    // Enhanced suffixes including more organizational terms
    let suffixes = [
        " program", " programs", " service", " services", " center", " centre",
        " clinic", " facility", " office", " department", " division", " unit",
        " hotline", " helpline", " line", " network", " system", " portal",
        " website", " site", " online", " virtual", " mobile", " app",
        " foundation", " organization", " org", " nonprofit", " non-profit",
        " charity", " charitable", " volunteer", " community", " local",
        " county", " state", " federal", " government", " gov", " public",
        " private", " inc", " incorporated", " llc", " ltd", " limited",
        " co", " corp", " corporation", " company", " enterprises",
        " associates", " group", " alliance", " coalition", " network",
        " initiative", " project", " effort", " campaign",
    ];
    
    for suffix in &suffixes {
        if result.ends_with(suffix) {
            result = result[..result.len() - suffix.len()].trim().to_string();
        }
    }
    
    result
}

fn final_service_cleanup(name: &str) -> String {
    // Remove multiple spaces and clean up
    let cleaned: String = name
        .chars()
        .filter(|c| c.is_alphanumeric() || c.is_whitespace())
        .collect();
    
    let normalized = cleaned
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_string();
    
    // Remove very short words that likely don't add semantic value
    let final_tokens: Vec<&str> = normalized
        .split_whitespace()
        .filter(|token| token.len() >= MIN_TOKEN_LENGTH || is_significant_short_token(token))
        .collect();
    
    final_tokens.join(" ")
}

fn is_significant_short_token(token: &str) -> bool {
    // Keep short tokens that are significant (numbers, specific abbreviations)
    if token.chars().all(|c| c.is_ascii_digit()) {
        return true; // Keep numbers
    }
    
    let significant_short = ["va", "er", "ob", "gyn", "ob/gyn", "er", "or", "icu", "ot", "pt"];
    significant_short.contains(&token)
}

/// Enhanced tokenization for services with location awareness
pub fn tokenize_service_name(normalized_name: &str, service_type: Option<&'static str>) -> (HashSet<String>, HashSet<String>) {
    let stopwords_set: HashSet<String> = SERVICE_STOPWORDS.iter().map(|&s| s.to_string()).collect();
    let mut tokens = HashSet::new();
    let mut bigrams = HashSet::new();
    let words: Vec<&str> = normalized_name.split_whitespace().collect();

    // Extract unigrams
    for word in &words {
        let token_str = word.to_lowercase();
        if !stopwords_set.contains(&token_str) && token_str.len() >= MIN_TOKEN_LENGTH {
            tokens.insert(token_str);
        }
    }

    // Extract bigrams
    for i in 0..words.len().saturating_sub(1) {
        let word1 = words[i].to_lowercase();
        let word2 = words[i + 1].to_lowercase();
        if !stopwords_set.contains(&word1) && !stopwords_set.contains(&word2)
           && word1.len() >= MIN_TOKEN_LENGTH && word2.len() >= MIN_TOKEN_LENGTH {
            bigrams.insert(format!("{} {}", word1, word2));
        }
    }

    // Add service type tokens if available
    if let Some(stype_str) = service_type {
        for type_token in stype_str.split('_') {
             if !stopwords_set.contains(type_token) && type_token.len() >= MIN_TOKEN_LENGTH {
                tokens.insert(type_token.to_string());
            }
        }
    }

    (tokens, bigrams)
}

/// NEW: Extract location information from service name
pub fn extract_location_info(normalized_name: &str) -> LocationInfo {
    let mut location_info = LocationInfo::default();
    
    // Extract specific location indicators
    for &indicator in &LOCATION_INDICATORS {
        if let Ok(re) = Regex::new(&format!(r"\b{}\b", regex::escape(indicator))) {
            if re.is_match(normalized_name) {
                location_info.indicators.insert(indicator.to_string());
                
                // Categorize location type
                match indicator {
                    "north" | "south" | "east" | "west" | "northeast" | "northwest" | "southeast" | "southwest" => {
                        location_info.directional = Some(indicator.to_string());
                    },
                    "county" | "parish" | "borough" => {
                        location_info.administrative = Some(indicator.to_string());
                    },
                    "downtown" | "uptown" | "midtown" | "central" => {
                        location_info.urban_area = Some(indicator.to_string());
                    },
                    _ => {}
                }
            }
        }
    }
    
    // Extract potential city/county names (simplified pattern)
    if let Ok(re) = Regex::new(r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+(?:county|city|township|borough)\b") {
        for cap in re.captures_iter(normalized_name) {
            if let Some(place_name) = cap.get(1) {
                location_info.place_names.insert(place_name.as_str().to_lowercase());
            }
        }
    }
    
    location_info
}

#[derive(Debug, Default, Clone)]
pub struct LocationInfo {
    pub indicators: HashSet<String>,
    pub directional: Option<String>,
    pub administrative: Option<String>,
    pub urban_area: Option<String>,
    pub place_names: HashSet<String>,
}

impl LocationInfo {
    fn calculate_similarity(&self, other: &LocationInfo) -> f64 {
        if self.indicators.is_empty() && other.indicators.is_empty() {
            return 1.0; // No location info, no penalty
        }
        
        let common_indicators = self.indicators.intersection(&other.indicators).count();
        let total_indicators = self.indicators.union(&other.indicators).count();
        
        let indicator_similarity = if total_indicators > 0 {
            common_indicators as f64 / total_indicators as f64
        } else {
            1.0
        };
        
        // Check for conflicting directional indicators
        if let (Some(ref dir1), Some(ref dir2)) = (&self.directional, &other.directional) {
            if dir1 != dir2 && are_conflicting_directions(dir1, dir2) {
                return 0.0; // Strong conflict
            }
        }
        
        // Check for matching place names
        let common_places = self.place_names.intersection(&other.place_names).count();
        let place_bonus = if common_places > 0 { 0.2 } else { 0.0 };
        
        (indicator_similarity + place_bonus).min(1.0)
    }
    
    fn has_conflicting_locations(&self, other: &LocationInfo) -> bool {
        // Check for conflicting directional indicators
        if let (Some(ref dir1), Some(ref dir2)) = (&self.directional, &other.directional) {
            if are_conflicting_directions(dir1, dir2) {
                return true;
            }
        }
        
        // Check for conflicting place names (simplified)
        if !self.place_names.is_empty() && !other.place_names.is_empty() {
            return self.place_names.is_disjoint(&other.place_names);
        }
        
        false
    }
}

fn are_conflicting_directions(dir1: &str, dir2: &str) -> bool {
    let conflicts = [
        ("north", "south"), ("east", "west"),
        ("northeast", "southwest"), ("northwest", "southeast"),
        ("northern", "southern"), ("eastern", "western"),
    ];
    
    conflicts.iter().any(|(d1, d2)| {
        (dir1 == *d1 && dir2 == *d2) || (dir1 == *d2 && dir2 == *d1)
    })
}

/// Enhanced service name matching with comprehensive location and contextual analysis
pub fn match_services_by_name_enhanced(service1: &ServiceInfo, service2: &ServiceInfo) -> Option<ServiceMatch> {
    if let (Some(name1_orig), Some(name2_orig)) = (&service1.name, &service2.name) {
        let (normalized_name1, service_type1) = normalize_service_name_advanced(name1_orig);
        let (normalized_name2, service_type2) = normalize_service_name_advanced(name2_orig);

        if normalized_name1.is_empty() || normalized_name2.is_empty() {
            return None;
        }

        // Check for incompatible service types early
        if let (Some(t1), Some(t2)) = (service_type1, service_type2) {
            if t1 != t2 && is_incompatible_service_types(t1, t2) {
                return None;
            }
        }

        // Extract location information
        let location1 = extract_location_info(&normalized_name1);
        let location2 = extract_location_info(&normalized_name2);
        
        // Check for conflicting locations early
        if location1.has_conflicting_locations(&location2) {
            return None; // Strong location conflict
        }

        let (tokens1, bigrams1) = tokenize_service_name(&normalized_name1, service_type1);
        let (tokens2, bigrams2) = tokenize_service_name(&normalized_name2, service_type2);

        let token_overlap = tokens1.intersection(&tokens2).count();
        if token_overlap < MIN_TOKEN_OVERLAP && bigrams1.intersection(&bigrams2).count() == 0 {
            return None;
        }

        // Calculate component similarities
        let fuzzy_score = jaro_winkler(&normalized_name1, &normalized_name2);
        let token_similarity_score = calculate_token_similarity(&tokens1, &tokens2, &bigrams1, &bigrams2);
        let location_similarity = location1.calculate_similarity(&location2);
        
        // Weighted combination including location
        let base_score = (fuzzy_score * FUZZY_WEIGHT) + (token_similarity_score * SEMANTIC_WEIGHT);
        let location_adjusted_score = base_score + (location_similarity * LOCATION_WEIGHT);
        
        let final_score = apply_enhanced_service_domain_rules(
            &normalized_name1, 
            &normalized_name2, 
            location_adjusted_score, 
            service_type1, 
            service_type2,
            &location1,
            &location2
        );

        if final_score >= SERVICE_NAME_COMBINED_THRESHOLD {
            return Some(ServiceMatch {
                service_id_1: service1.service_id.clone(),
                service_id_2: service2.service_id.clone(),
                match_method: MatchMethodType::ServiceNameSimilarity,
                confidence_score: final_score.min(1.0),
                match_values: MatchValues::ServiceName(ServiceNameMatchValue {
                    original_name1: name1_orig.clone(),
                    original_name2: name2_orig.clone(),
                    normalized_name1,
                    normalized_name2,
                }),
            });
        }
    }
    None
}

fn is_incompatible_service_types(type1: &str, type2: &str) -> bool {
    INCOMPATIBLE_SERVICE_TYPES.iter().any(|(t1, t2)| {
        (type1 == *t1 && type2 == *t2) || (type1 == *t2 && type2 == *t1)
    })
}

fn calculate_token_similarity(
    tokens1: &HashSet<String>, tokens2: &HashSet<String>,
    bigrams1: &HashSet<String>, bigrams2: &HashSet<String>
) -> f64 {
    let unigram_intersection = tokens1.intersection(tokens2).count();
    let unigram_union = tokens1.union(tokens2).count();
    let unigram_similarity = if unigram_union > 0 { 
        unigram_intersection as f64 / unigram_union as f64 
    } else { 
        0.0 
    };

    let bigram_intersection = bigrams1.intersection(bigrams2).count();
    let bigram_union = bigrams1.union(bigrams2).count();
    let bigram_similarity = if bigram_union > 0 { 
        bigram_intersection as f64 / bigram_union as f64 
    } else { 
        0.0 
    };

    // Give more weight to bigram similarity if present
    if bigram_intersection > 0 {
        (unigram_similarity * 0.4) + (bigram_similarity * 0.6)
    } else {
        unigram_similarity
    }
}

/// Enhanced domain rules with location awareness and contextual analysis
fn apply_enhanced_service_domain_rules(
    name1: &str, 
    name2: &str, 
    base_score: f64,
    type1: Option<&'static str>, 
    type2: Option<&'static str>,
    location1: &LocationInfo,
    location2: &LocationInfo
) -> f64 {
    let mut adjusted_score = base_score;

    // Service type bonuses
    if let (Some(t1), Some(t2)) = (type1, type2) {
        if t1 == t2 { 
            adjusted_score *= 1.12; // Increased bonus for same detected type
        }
    }

    // Length ratio penalty (refined)
    let len1 = name1.split_whitespace().count();
    let len2 = name2.split_whitespace().count();
    let length_ratio = len1.max(len2) as f64 / len1.min(len2).max(1) as f64;
    if length_ratio > 3.0 { 
        adjusted_score *= 0.80; // Stronger penalty for very different lengths
    } else if length_ratio > 2.0 {
        adjusted_score *= 0.90; // Moderate penalty
    }

    // Emergency service handling (enhanced)
    let emergency_indicators = ["emergency", "crisis", "urgent", "hotline", "911", "211", "24/7", "24hr"];
    let name1_is_emergency = emergency_indicators.iter().any(|&ind| name1.contains(ind));
    let name2_is_emergency = emergency_indicators.iter().any(|&ind| name2.contains(ind));
    
    if name1_is_emergency && name2_is_emergency { 
        adjusted_score *= 1.18; // Strong boost for both emergency
    } else if name1_is_emergency != name2_is_emergency { 
        adjusted_score *= 0.85; // Penalty if only one is emergency
    }

    // Location-specific adjustments
    if location1.has_conflicting_locations(location2) {
        adjusted_score *= (1.0 - LOCATION_CONFLICT_PENALTY);
    } else if !location1.place_names.is_empty() && !location2.place_names.is_empty() {
        let common_places = location1.place_names.intersection(&location2.place_names).count();
        if common_places > 0 {
            adjusted_score *= (1.0 + LOCATION_MATCH_BONUS);
        }
    }

    // Program-specific handling (enhanced)
    let program_indicators = [
        "snap", "wic", "tanf", "liheap", "medicaid", "medicare", "ebt",
        "section 8", "head start", "meals on wheels", "211", "311"
    ];
    let prog1_indicators: Vec<_> = program_indicators.iter()
        .filter(|&&prog| name1.contains(prog))
        .collect();
    let prog2_indicators: Vec<_> = program_indicators.iter()
        .filter(|&&prog| name2.contains(prog))
        .collect();
    
    if !prog1_indicators.is_empty() && !prog2_indicators.is_empty() {
        let shared_programs = prog1_indicators.iter()
            .filter(|&prog1| prog2_indicators.iter().any(|&prog2| prog1.to_owned() == prog2))
            .count();
        if shared_programs > 0 { 
            adjusted_score *= 1.25; // Strong boost for same specific program
        } else { 
            adjusted_score *= 0.75; // Penalty for different specific programs
        }
    } else if prog1_indicators.len() != prog2_indicators.len() {
        adjusted_score *= 0.88; // Slight penalty if one is program-specific and other isn't
    }

    // Numeric/code pattern handling
    if contains_numeric_pattern(name1) && contains_numeric_pattern(name2) {
        let numeric_similarity = calculate_numeric_similarity(name1, name2);
        if numeric_similarity > 0.8 {
            adjusted_score *= 1.10; // Boost for similar numeric patterns
        } else if numeric_similarity < 0.3 {
            adjusted_score *= 0.80; // Penalty for very different numeric patterns
        }
    }

    adjusted_score.min(1.0)
}

fn contains_numeric_pattern(name: &str) -> bool {
    NUMERIC_PATTERNS.iter().any(|(pattern, _)| {
        if let Ok(re) = Regex::new(pattern) {
            re.is_match(name)
        } else {
            false
        }
    })
}

fn calculate_numeric_similarity(name1: &str, name2: &str) -> f64 {
    // Extract numbers from both names
    let re = Regex::new(r"\d+").unwrap();
    let nums1: HashSet<_> = re.find_iter(name1).map(|m| m.as_str()).collect();
    let nums2: HashSet<_> = re.find_iter(name2).map(|m| m.as_str()).collect();
    
    if nums1.is_empty() && nums2.is_empty() {
        return 1.0;
    }
    
    let common = nums1.intersection(&nums2).count();
    let total = nums1.union(&nums2).count();
    
    if total > 0 {
        common as f64 / total as f64
    } else {
        0.0
    }
}

/// Enhanced similarity function for backward compatibility
pub fn calculate_string_similarity_enhanced(s1: &str, s2: &str) -> f64 {
    if s1 == s2 { return 1.0; }
    if s1.is_empty() || s2.is_empty() { return 0.0; }

    let (norm1, type1) = normalize_service_name_advanced(s1);
    let (norm2, type2) = normalize_service_name_advanced(s2);

    if norm1.is_empty() || norm2.is_empty() { return 0.0; }

    if let (Some(t1), Some(t2)) = (type1, type2) {
        if t1 != t2 && is_incompatible_service_types(t1, t2) {
            return 0.0;
        }
    }

    let location1 = extract_location_info(&norm1);
    let location2 = extract_location_info(&norm2);
    
    if location1.has_conflicting_locations(&location2) {
        return 0.0;
    }

    let (tokens1, bigrams1) = tokenize_service_name(&norm1, type1);
    let (tokens2, bigrams2) = tokenize_service_name(&norm2, type2);

    let fuzzy_score = jaro_winkler(&norm1, &norm2);
    let token_similarity = calculate_token_similarity(&tokens1, &tokens2, &bigrams1, &bigrams2);
    let location_similarity = location1.calculate_similarity(&location2);
    
    let base_score = (fuzzy_score * FUZZY_WEIGHT) + (token_similarity * SEMANTIC_WEIGHT);
    let location_adjusted_score = base_score + (location_similarity * LOCATION_WEIGHT);

    apply_enhanced_service_domain_rules(&norm1, &norm2, location_adjusted_score, type1, type2, &location1, &location2)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_enhanced_service_name_normalization() {
        assert_eq!(
            normalize_service_name_advanced("North Seattle Food Bank - Emergency Services (Main Office)").0,
            "north seattle food bank emergency main"
        );
        assert_eq!(
            normalize_service_name_advanced("Route 66 Crisis Hotline Program").0,
            "route 66 crisis hotline"
        );
        assert_eq!(
            normalize_service_name_advanced("Community Mental Health Center, Unit A-123").0,
            "mental health unit a 123"
        );
    }

    #[test]
    fn test_location_extraction() {
        let location = extract_location_info("north seattle community health center");
        assert!(location.indicators.contains("north"));
        assert!(location.indicators.contains("community"));
        assert_eq!(location.directional, Some("north".to_string()));
    }

    #[test]
    fn test_conflicting_locations() {
        let loc1 = extract_location_info("north side food bank");
        let loc2 = extract_location_info("south side food bank");
        assert!(loc1.has_conflicting_locations(&loc2));
        
        let loc3 = extract_location_info("central food bank");
        let loc4 = extract_location_info("downtown food bank");
        assert!(!loc3.has_conflicting_locations(&loc4));
    }

    #[test]
    fn test_enhanced_similarity() {
        assert!(calculate_string_similarity_enhanced(
            "North Side Food Bank Emergency Services", 
            "North Side Emergency Food Pantry"
        ) > 0.85);
        
        assert!(calculate_string_similarity_enhanced(
            "North Side Food Bank", 
            "South Side Food Bank"
        ) < 0.5); // Should be penalized for conflicting locations
        
        assert!(calculate_string_similarity_enhanced(
            "SNAP Benefits Office Route 5", 
            "SNAP Application Center Route 5"
        ) > 0.90); // Should get boost for matching route number
    }

    #[test]
    fn test_numeric_pattern_handling() {
        assert!(contains_numeric_pattern("route 66 services"));
        assert!(contains_numeric_pattern("program a-123"));
        assert!(!contains_numeric_pattern("general services"));
        
        assert!(calculate_numeric_similarity("route 66", "route 66") > 0.9);
        assert!(calculate_numeric_similarity("route 66", "route 67") < 0.5);
    }
}