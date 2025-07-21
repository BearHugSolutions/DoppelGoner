// src/matching/name.rs
// Enhanced service name matching strategies with advanced location handling, contextual rules, and conditional RL integration.

use log::{debug, info, warn};
use regex::Regex;
use std::{collections::{HashMap, HashSet}, sync::Arc, time::Instant};
use strsim::jaro_winkler;
use tokio::sync::Mutex;
use indicatif::ProgressBar;

use crate::models::{
    matching::{MatchMethodType, MatchValues, ServiceInfo, ServiceMatch, ServiceNameMatchValue}, 
    stats_models::ServiceNameMatchingStats
};
use crate::rl::{ServiceRLOrchestrator, SharedServiceFeatureCache};
use crate::utils::db_connect::PgPool;

// Enhanced configuration constants
const SERVICE_NAME_FUZZY_THRESHOLD: f64 = 0.88;
const SERVICE_NAME_COMBINED_THRESHOLD: f64 = 0.85;
const FUZZY_WEIGHT: f64 = 0.4;
const SEMANTIC_WEIGHT: f64 = 0.6;
const MIN_TOKEN_LENGTH: usize = 2;
const MIN_TOKEN_OVERLAP: usize = 2;

// Location handling constants
const LOCATION_WEIGHT: f64 = 0.15;
const LOCATION_CONFLICT_PENALTY: f64 = 0.25;
const LOCATION_MATCH_BONUS: f64 = 0.08;

const LARGE_CLUSTER_THRESHOLD_NAME: usize = 500;

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

// Enhanced location indicators with regional specificity
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

// Numeric/code pattern handlers
const NUMERIC_PATTERNS: [(&str, &str); 8] = [
    (r"\broute\s+(\d+)", "route $1"),
    (r"\bhighway\s+(\d+)", "highway $1"),
    (r"\bprogram\s+([a-z])-?(\d+)", "program $1 $2"),
    (r"\bunit\s+(\d+[a-z]?)", "unit $1"),
    (r"\bsite\s+(\d+)", "site $1"),
    (r"\bbuilding\s+(\d+[a-z]?)", "building $1"),
    (r"\broom\s+(\d+[a-z]?)", "room $1"),
    (r"\bfloor\s+(\d+)", "floor $1"),
];


/// Entry point for service name matching with conditional RL integration
pub async fn run_enhanced_name_matching(
    pool: &PgPool,
    services: &[ServiceInfo],
    rl_orchestrator: Option<Arc<Mutex<ServiceRLOrchestrator>>>,
    feature_cache: Option<SharedServiceFeatureCache>,
    method_pb: Option<ProgressBar>,
) -> (Vec<ServiceMatch>, Vec<(String, String, Vec<f64>)>, ServiceNameMatchingStats) {
    let start_time = Instant::now();
    let mut stats = ServiceNameMatchingStats::default();
    let mut features_for_rl_logging: Vec<(String, String, Vec<f64>)> = Vec::new();

    let rl_enabled = rl_orchestrator.is_some();
    debug!("Starting service name matching with {} services (RL: {})", services.len(), if rl_enabled { "enabled" } else { "disabled" });

    if services.len() > LARGE_CLUSTER_THRESHOLD_NAME {
        stats.large_cluster_mode = true;
        warn!("Running Service Name matching in LARGE CLUSTER (indexing) mode ({} services, RL: {})", services.len(), if rl_enabled { "enabled" } else { "disabled" });
        if let Some(pb) = &method_pb {
            pb.set_length(2);
            pb.set_message(format!("Name Matching: Building index (large cluster, RL: {})...", if rl_enabled { "on" } else { "off" }));
            pb.set_position(0);
        }
        let (matches, rl_features, indexed_stats) = run_indexed_name_matching(pool, services, rl_orchestrator, feature_cache, method_pb.clone()).await;
        stats.matches_found = indexed_stats.matches_found;
        stats.fuzzy_matches = indexed_stats.fuzzy_matches;
        stats.token_matches = indexed_stats.token_matches;
        stats.same_type_matches = indexed_stats.same_type_matches;
        stats.incompatible_type_rejections = indexed_stats.incompatible_type_rejections;
        stats.avg_confidence = indexed_stats.avg_confidence;
        stats.pairs_compared = indexed_stats.pairs_compared;
        stats.processing_time = start_time.elapsed();
        if let Some(pb) = method_pb { pb.finish_and_clear(); }
        return (matches, rl_features, stats);
    }

    // Original O(N^2) logic for smaller clusters
    let mut matches = Vec::new();
    let mut confidence_scores = Vec::new();
    
    if let Some(pb) = &method_pb {
        pb.set_length(services.len() as u64 * services.len() as u64 / 2);
        pb.set_message(format!("Name Matching: Comparing all pairs (RL: {})...", if rl_enabled { "on" } else { "off" }));
        pb.set_position(0);
    }

    let mut pair_count_for_pb = 0;
    for i in 0..services.len() {
        for j in (i + 1)..services.len() {
            stats.pairs_compared += 1;
            
            // Get a fresh connection for this pair
            let conn = match pool.get().await {
                Ok(conn) => conn,
                Err(e) => {
                    warn!("Failed to get connection for pair ({}, {}): {}", 
                          services[i].service_id, services[j].service_id, e);
                    continue;
                }
            };
            
            if let Some((name_match, features_opt)) = match_services_by_name_enhanced(
                &services[i], 
                &services[j], 
                &rl_orchestrator, 
                &feature_cache, 
                &*conn
            ).await {
                matches.push(name_match.clone());
                confidence_scores.push(name_match.confidence_score);
                
                if name_match.confidence_score >= 0.9 {
                    stats.fuzzy_matches += 1;
                }
                
                if let MatchValues::ServiceName(ref name_values) = name_match.match_values {
                    let (_, type1) = normalize_service_name_advanced(&name_values.original_name1);
                    let (_, type2) = normalize_service_name_advanced(&name_values.original_name2);
                    if type1.is_some() && type1 == type2 {
                        stats.same_type_matches += 1;
                    }
                }

                if let Some(features) = features_opt {
                    let (s1_id, s2_id) = if services[i].service_id <= services[j].service_id {
                        (services[i].service_id.clone(), services[j].service_id.clone())
                    } else {
                        (services[j].service_id.clone(), services[i].service_id.clone())
                    };
                    features_for_rl_logging.push((s1_id, s2_id, features));
                }
            }
            
            // Connection is automatically dropped here when it goes out of scope
            
            if let Some(pb) = &method_pb { 
                pair_count_for_pb += 1;
                pb.set_position(pair_count_for_pb);
            }
        }
    }
    
    stats.matches_found = matches.len();
    stats.avg_confidence = if !confidence_scores.is_empty() { 
        confidence_scores.iter().sum::<f64>() / confidence_scores.len() as f64 
    } else { 
        0.0 
    };
    stats.processing_time = start_time.elapsed();
    
    info!(
        "Service name matching completed: {} matches from {} pairs in {:.2?} (RL: {})",
        stats.matches_found,
        stats.pairs_compared,
        stats.processing_time,
        if rl_enabled { "enabled" } else { "disabled" }
    );
    
    if let Some(pb) = method_pb { pb.finish_and_clear(); }

    (matches, features_for_rl_logging, stats)
}

/// Runs indexed name matching for large clusters, now with conditional RL integration.
async fn run_indexed_name_matching(
    pool: &PgPool,
    services: &[ServiceInfo],
    rl_orchestrator: Option<Arc<Mutex<ServiceRLOrchestrator>>>,
    feature_cache: Option<SharedServiceFeatureCache>,
    method_pb: Option<ProgressBar>,
) -> (Vec<ServiceMatch>, Vec<(String, String, Vec<f64>)>, ServiceNameMatchingStats) {
    let mut matches = Vec::new();
    let mut stats = ServiceNameMatchingStats::default();
    let mut confidence_scores = Vec::new();
    let mut features_for_rl_logging: Vec<(String, String, Vec<f64>)> = Vec::new();
    let mut inverted_index: HashMap<String, Vec<usize>> = HashMap::new();
    let mut service_data_map: HashMap<usize, &ServiceInfo> = HashMap::new();

    // 1. Build Inverted Index & Store ServiceData
    if let Some(pb) = &method_pb {
        pb.set_message("Name Matching: Building inverted index...");
        pb.set_length(services.len() as u64);
        pb.set_position(0);
    }
    for (idx, service) in services.iter().enumerate() {
        service_data_map.insert(idx, service);
        if let Some(name) = &service.name {
            let (normalized, stype) = normalize_service_name_advanced(name);
            let (tokens, _bigrams) = tokenize_service_name(&normalized, stype);
            for token in tokens {
                inverted_index.entry(token).or_default().push(idx);
            }
        }
        if let Some(pb) = &method_pb { pb.inc(1); }
    }
    if let Some(pb) = &method_pb { pb.set_message("Name Matching: Index built, generating candidates..."); }

    // 2. Generate Candidate Pairs
    let mut candidate_pairs: HashSet<(usize, usize)> = HashSet::new();
    for (_token, service_indices) in inverted_index {
        if service_indices.len() >= 2 && service_indices.len() < 1000 {
            for i in 0..service_indices.len() {
                for j in (i + 1)..service_indices.len() {
                    let idx1 = service_indices[i];
                    let idx2 = service_indices[j];
                    candidate_pairs.insert(if idx1 < idx2 { (idx1, idx2) } else { (idx2, idx1) });
                }
            }
        }
    }

    // 3. Process Candidate Pairs
    if let Some(pb) = &method_pb {
        pb.set_message("Name Matching: Processing candidate pairs...");
        pb.set_length(candidate_pairs.len() as u64);
        pb.set_position(0);
    }
    for (idx1, idx2) in candidate_pairs {
        stats.pairs_compared += 1;
        let service1 = service_data_map[&idx1];
        let service2 = service_data_map[&idx2];
        
        let conn = match pool.get().await {
            Ok(conn) => conn,
            Err(e) => {
                warn!("Failed to get connection for pair ({}, {}): {}", 
                      service1.service_id, service2.service_id, e);
                continue;
            }
        };

        if let Some((name_match, features_opt)) = match_services_by_name_enhanced(
            service1, 
            service2, 
            &rl_orchestrator, 
            &feature_cache, 
            &*conn
        ).await {
            matches.push(name_match.clone());
            confidence_scores.push(name_match.confidence_score);
            if let Some(features) = features_opt {
                let (s1_id, s2_id) = if service1.service_id <= service2.service_id {
                    (service1.service_id.clone(), service2.service_id.clone())
                } else {
                    (service2.service_id.clone(), service1.service_id.clone())
                };
                features_for_rl_logging.push((s1_id, s2_id, features));
            }
        }
        
        if let Some(pb) = &method_pb { pb.inc(1); }
    }

    stats.matches_found = matches.len();
    stats.avg_confidence = if !confidence_scores.is_empty() { 
        confidence_scores.iter().sum::<f64>() / confidence_scores.len() as f64 
    } else { 
        0.0 
    };
    
    (matches, features_for_rl_logging, stats)
}

/// Enhanced service name matching with comprehensive location and contextual analysis plus conditional RL integration
pub async fn match_services_by_name_enhanced(
    service1: &ServiceInfo, 
    service2: &ServiceInfo,
    rl_orchestrator: &Option<Arc<Mutex<ServiceRLOrchestrator>>>,
    feature_cache: &Option<SharedServiceFeatureCache>,
    conn: &tokio_postgres::Client,
) -> Option<(ServiceMatch, Option<Vec<f64>>)> {
    if let (Some(name1_orig), Some(name2_orig)) = (&service1.name, &service2.name) {
        let (normalized_name1, service_type1) = normalize_service_name_advanced(name1_orig);
        let (normalized_name2, service_type2) = normalize_service_name_advanced(name2_orig);

        if normalized_name1.is_empty() || normalized_name2.is_empty() {
            return None;
        }

        if let (Some(t1), Some(t2)) = (service_type1, service_type2) {
            if t1 != t2 && is_incompatible_service_types(t1, t2) {
                debug!("Rejecting service name match due to incompatible types: {} vs {}", t1, t2);
                return None;
            }
        }

        let location1 = extract_location_info(&normalized_name1);
        let location2 = extract_location_info(&normalized_name2);
        
        if location1.has_conflicting_locations(&location2) {
            debug!("Rejecting service name match due to conflicting locations");
            return None;
        }

        let (tokens1, bigrams1) = tokenize_service_name(&normalized_name1, service_type1);
        let (tokens2, bigrams2) = tokenize_service_name(&normalized_name2, service_type2);

        let token_overlap = tokens1.intersection(&tokens2).count();
        if token_overlap < MIN_TOKEN_OVERLAP && bigrams1.intersection(&bigrams2).count() == 0 {
            return None;
        }

        let fuzzy_score = jaro_winkler(&normalized_name1, &normalized_name2);
        let token_similarity_score = calculate_token_similarity(&tokens1, &tokens2, &bigrams1, &bigrams2);
        let location_similarity = location1.calculate_similarity(&location2);
        
        let base_score = (fuzzy_score * FUZZY_WEIGHT) + (token_similarity_score * SEMANTIC_WEIGHT);
        let location_adjusted_score = base_score + (location_similarity * LOCATION_WEIGHT);
        
        let pre_rl_confidence = apply_enhanced_service_domain_rules(
            &normalized_name1, 
            &normalized_name2, 
            location_adjusted_score, 
            service_type1, 
            service_type2,
            &location1,
            &location2
        );

        if pre_rl_confidence < SERVICE_NAME_COMBINED_THRESHOLD {
            return None;
        }

        let mut captured_features: Option<Vec<f64>> = None;
        let rl_enabled = rl_orchestrator.is_some();

        // MODIFIED: Conditional RL Integration
        let final_confidence = if rl_enabled {
            if let Some(rl_orch) = rl_orchestrator {
                // Extract features (always useful for caching)
                let context_features_res = if let Some(cache_arc) = feature_cache {
                    let mut cache_guard = cache_arc.lock().await;
                    cache_guard.get_pair_features(conn, &service1.service_id, &service2.service_id).await
                } else {
                    crate::rl::service_feature_extraction::extract_service_context_for_pair(
                        conn, &service1.service_id, &service2.service_id
                    ).await
                };

                if let Ok(features) = context_features_res {
                    captured_features = Some(features.clone());
                    let mut rl_guard = rl_orch.lock().await;
                    match rl_guard.get_tuned_confidence(
                        &MatchMethodType::ServiceNameSimilarity,
                        pre_rl_confidence,
                        &features,
                    ) {
                        Ok(tuned_conf) => {
                            debug!(
                                "Service name match RL tuning: {:.3} -> {:.3} for services {} <-> {}",
                                pre_rl_confidence, tuned_conf, service1.service_id, service2.service_id
                            );
                            tuned_conf
                        }
                        Err(e) => {
                            warn!("Failed to get RL tuned confidence for service name match: {}. Using pre-RL confidence.", e);
                            pre_rl_confidence
                        }
                    }
                } else {
                    warn!("Failed to extract features for service pair ({}, {}): {}. Using pre-RL confidence.", 
                            service1.service_id, service2.service_id, context_features_res.unwrap_err());
                    pre_rl_confidence
                }
            } else {
                // This shouldn't happen if rl_enabled is true, but handle gracefully
                warn!("RL enabled but no orchestrator available, using pre-RL confidence");
                pre_rl_confidence
            }
        } else {
            // RL disabled - still extract features for caching if cache is available
            if let Some(cache_arc) = feature_cache {
                let context_features_res = {
                    let mut cache_guard = cache_arc.lock().await;
                    cache_guard.get_pair_features(conn, &service1.service_id, &service2.service_id).await
                };
                
                if let Ok(features) = context_features_res {
                    captured_features = Some(features);
                    debug!("Extracted features for caching (RL disabled) for pair ({}, {})", 
                           service1.service_id, service2.service_id);
                }
            }
            pre_rl_confidence
        };

        if final_confidence >= SERVICE_NAME_COMBINED_THRESHOLD {
            debug!(
                "Service name match: {} <-> {} (pre-RL: {:.3}, final: {:.3}, RL: {})",
                service1.service_id, service2.service_id, pre_rl_confidence, final_confidence,
                if rl_enabled { "enabled" } else { "disabled" }
            );
            
            Some((ServiceMatch {
                service_id_1: service1.service_id.clone(),
                service_id_2: service2.service_id.clone(),
                match_method: MatchMethodType::ServiceNameSimilarity,
                confidence_score: final_confidence.min(1.0),
                match_values: MatchValues::ServiceName(ServiceNameMatchValue {
                    original_name1: name1_orig.clone(),
                    original_name2: name2_orig.clone(),
                    normalized_name1,
                    normalized_name2,
                }),
            }, captured_features))
        } else {
            None
        }
    } else {
        None
    }
}

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

// Handle numeric and code patterns specially
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

/// Extract location information from service name
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
