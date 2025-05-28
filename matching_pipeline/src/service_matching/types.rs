// src/service_matching/types.rs
// Enhanced types supporting cross-validation, aggregation, and improved matching strategies

use std::collections::{HashMap, HashSet};

use crate::{models::{MatchValues, ServiceId}, EntityId, MatchMethodType};

#[derive(Debug, Clone)]
pub struct ServiceInfo {
    pub service_id: ServiceId,
    pub entity_id: EntityId,
    pub name: Option<String>,
    pub email: Option<String>,
    pub url: Option<String>,
    pub embedding: Option<Vec<f32>>,
}

#[derive(Debug, Clone)]
pub struct ServiceMatch {
    pub service_id_1: ServiceId,
    pub service_id_2: ServiceId,
    pub match_method: MatchMethodType,
    pub confidence_score: f64,
    pub match_values: MatchValues,
}

#[derive(Debug, Clone)]
pub struct ServiceEmailMatch {
    pub service_id_1: ServiceId,
    pub service_id_2: ServiceId,
    pub confidence_score: f64,
    pub match_values: MatchValues,
    pub frequency_adjustment: f64,
    pub generic_email_detected: bool,
}

#[derive(Debug, Default, Clone)]
pub struct ServiceEmailMatchingStats {
    pub services_processed: usize,
    pub pairs_compared: usize,
    pub matches_found: usize,
    pub generic_email_matches: usize,
    pub high_frequency_matches: usize,
    pub avg_confidence: f64,
    pub processing_time: std::time::Duration,
}

#[derive(Clone, Debug)]
pub struct ServiceUrlMatch {
    pub service_id_1: ServiceId,
    pub service_id_2: ServiceId,
    pub confidence_score: f64,
    pub match_values: MatchValues,
}

/// Enhanced URL structure with query parameters and fragment support
#[derive(Clone, Debug)]
pub struct NormalizedServiceUrl {
    pub domain: String,
    pub path_slugs: Vec<String>,
    pub original_url: String,
    pub domain_type: ServiceDomainType,
    pub subdomain: Option<String>,
    pub is_https: bool,
    // NEW: Enhanced URL components
    pub significant_query_params: Option<HashMap<String, String>>,
    pub fragment: Option<String>,
}

#[derive(Clone, PartialEq, Debug)]
pub enum ServiceDomainType {
    Government,     // .gov, .mil
    Healthcare,     // health, hospital, clinic keywords or .org
    Education,      // .edu, k12, university keywords
    Commercial,     // .com, .net, .biz
    Social,         // Social media platforms
    CloudService,   // Cloud hosting services
    GenericPlatform,// Generic website builders
    Other,
}

// NEW: Enhanced service name data for better token matching
#[derive(Debug, Clone)]
pub struct ServiceNameData {
    pub original_name: String,
    pub normalized_name: String,
    pub service_type: Option<&'static str>,
    pub tokens: HashSet<String>,
    pub bigrams: HashSet<String>,
    pub location_info: LocationInfo,
}

/// NEW: Location information extracted from service names
#[derive(Debug, Default, Clone)]
pub struct LocationInfo {
    pub indicators: HashSet<String>,
    pub directional: Option<String>,
    pub administrative: Option<String>,
    pub urban_area: Option<String>,
    pub place_names: HashSet<String>,
}

impl LocationInfo {
    /// Calculate similarity between two location info objects
    pub fn calculate_similarity(&self, other: &LocationInfo) -> f64 {
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
            if dir1 != dir2 && Self::are_conflicting_directions(dir1, dir2) {
                return 0.0; // Strong conflict
            }
        }
        
        // Check for matching place names
        let common_places = self.place_names.intersection(&other.place_names).count();
        let place_bonus = if common_places > 0 { 0.2 } else { 0.0 };
        
        (indicator_similarity + place_bonus).min(1.0)
    }
    
    /// Check if this location conflicts with another
    pub fn has_conflicting_locations(&self, other: &LocationInfo) -> bool {
        // Check for conflicting directional indicators
        if let (Some(ref dir1), Some(ref dir2)) = (&self.directional, &other.directional) {
            if Self::are_conflicting_directions(dir1, dir2) {
                return true;
            }
        }
        
        // Check for conflicting place names (simplified)
        if !self.place_names.is_empty() && !other.place_names.is_empty() {
            return self.place_names.is_disjoint(&other.place_names);
        }
        
        false
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
}

// NEW: Cross-validation and aggregation support types

/// Enhanced match data for cross-validation and aggregation
#[derive(Debug, Clone)]
pub struct EnhancedServiceMatch {
    pub base_match: ServiceMatch,
    pub method_matches: Vec<MethodMatch>, // All methods that found this pair
    pub cross_validation_score: f64, // Penalty/bonus from cross-validation
    pub final_confidence: f64, // Aggregated confidence
}

/// Individual method match with validation flags
#[derive(Debug, Clone)]
pub struct MethodMatch {
    pub method: MatchMethodType,
    pub confidence: f64,
    pub match_values: MatchValues,
    pub validation_flags: ValidationFlags,
}

/// Validation flags for cross-validation logic
#[derive(Debug, Clone, Default)]
pub struct ValidationFlags {
    pub incompatible_types: bool,
    pub conflicting_locations: bool,
    pub generic_indicators: bool,
    pub suspicious_patterns: bool,
}

/// Result of cross-validation analysis
#[derive(Debug)]
pub struct CrossValidationResult {
    pub should_reject: bool,
    pub rejection_reason: Option<String>,
    pub confidence_modifier: f64, // Multiplier for confidence (1.0 = no change, >1.0 = boost, <1.0 = penalty)
}

// NEW: Email domain analysis types

/// Domain trust analysis for email matching
#[derive(Debug, Clone)]
pub struct DomainTrustScore {
    pub is_organizational: bool,
    pub is_free_provider: bool,
    pub is_governmental: bool,
    pub is_educational: bool,
    pub is_healthcare: bool,
    pub specificity_score: f64, // 0.0 to 1.0, higher = more specific
    pub trust_adjustment: f64, // Positive = bonus, negative = penalty
}

/// Comprehensive email analysis result
#[derive(Debug, Clone)]
pub struct EmailAnalysis {
    pub is_highly_generic: bool,
    pub is_role_based: bool,
    pub is_department_specific: bool,
    pub generic_penalty: f64,
    pub domain_trust_adjustment: f64,
    pub domain_info: Option<DomainTrustScore>,
}

// NEW: Enhanced statistics with cross-validation metrics

/// Enhanced URL matching statistics
#[derive(Debug, Default, Clone)]
pub struct UrlMatchingStats {
    pub matches_found: usize,
    pub domain_only_matches: usize,
    pub path_matches: usize,
    pub query_param_matches: usize, // NEW
    pub fragment_matches: usize, // NEW
    pub ignored_domains: usize,
    pub avg_confidence: f64,
    pub processing_time: std::time::Duration,
    pub pairs_compared: usize,
    pub large_cluster_mode: bool,
    // NEW: Enhanced metrics
    pub weighted_slug_matches: usize,
    pub domain_trust_adjustments: usize,
}

/// Enhanced name matching statistics
#[derive(Debug, Default, Clone)]
pub struct NameMatchingStats {
    pub matches_found: usize,
    pub fuzzy_matches: usize,
    pub token_matches: usize,
    pub same_type_matches: usize,
    pub incompatible_type_rejections: usize,
    pub location_conflict_rejections: usize, // NEW
    pub numeric_pattern_matches: usize, // NEW
    pub avg_confidence: f64,
    pub processing_time: std::time::Duration,
    pub pairs_compared: usize,
    pub large_cluster_mode: bool,
}

/// Enhanced embedding matching statistics
#[derive(Debug, Default, Clone)]
pub struct EmbeddingMatchingStats {
    pub matches_found: usize,
    pub pairs_compared: usize,
    pub avg_similarity: f64,
    pub processing_time: std::time::Duration,
    pub skipped_large_cluster: bool,
    // NEW: Enhanced metrics
    pub contextual_boosts: usize, // Matches that got contextual confidence boosts
    pub type_compatibility_checks: usize, // Pairs that had service type compatibility checked
}

/// Enhanced overall matching statistics
#[derive(Debug, Default, Clone)]
pub struct ServiceMatchingStats {
    pub total_clusters_processed: usize,
    pub total_services_compared: usize,
    pub total_matches_found: usize,
    pub matches_by_method: HashMap<MatchMethodType, usize>,
    pub processing_time: std::time::Duration,
    
    // Individual strategy stats
    pub email_stats: Option<ServiceEmailMatchingStats>,
    pub url_stats: UrlMatchingStats,
    pub name_stats: NameMatchingStats,
    pub embedding_stats: EmbeddingMatchingStats,
    
    // NEW: Cross-validation and aggregation stats
    pub aggregated_matches: usize,
    pub cross_validation_rejections: usize,
    pub confidence_boosts: usize,
    pub multi_method_matches: usize, // Pairs matched by multiple methods
    pub single_method_matches: usize, // Pairs matched by only one method
}

// NEW: Query parameter analysis types for URL matching

/// Significant query parameter for URL matching
#[derive(Debug, Clone)]
pub struct QueryParameter {
    pub key: String,
    pub value: String,
    pub significance_weight: f64, // How significant this parameter is for matching
}

/// Query parameter comparison result
#[derive(Debug, Clone)]
pub struct QueryParamComparison {
    pub matching_params: Vec<QueryParameter>,
    pub conflicting_params: Vec<(QueryParameter, QueryParameter)>,
    pub unique_params_1: Vec<QueryParameter>,
    pub unique_params_2: Vec<QueryParameter>,
    pub similarity_score: f64,
}

// NEW: Fragment analysis types for URL matching

/// URL fragment analysis
#[derive(Debug, Clone)]
pub struct FragmentAnalysis {
    pub fragment_1: Option<String>,
    pub fragment_2: Option<String>,
    pub are_related: bool,
    pub confidence_adjustment: f64,
}

// NEW: Service type analysis for enhanced matching

/// Service type compatibility analysis
#[derive(Debug, Clone)]
pub struct ServiceTypeCompatibility {
    pub type_1: Option<String>,
    pub type_2: Option<String>,
    pub are_compatible: bool,
    pub are_identical: bool,
    pub compatibility_score: f64, // 0.0 = incompatible, 1.0 = identical
}

// NEW: Numeric pattern analysis for name matching

/// Numeric pattern found in service names
#[derive(Debug, Clone)]
pub struct NumericPattern {
    pub pattern_type: NumericPatternType,
    pub value: String,
    pub position: usize, // Position in the name where found
}

#[derive(Debug, Clone, PartialEq)]
pub enum NumericPatternType {
    Route,      // Route 66
    Highway,    // Highway 101
    Program,    // Program A-123
    Unit,       // Unit 5A
    Site,       // Site 3
    Building,   // Building 2B
    Room,       // Room 105A
    Floor,      // Floor 3
    Other,      // Other numeric patterns
}

/// Comparison result for numeric patterns
#[derive(Debug, Clone)]
pub struct NumericPatternComparison {
    pub patterns_1: Vec<NumericPattern>,
    pub patterns_2: Vec<NumericPattern>,
    pub matching_patterns: Vec<(NumericPattern, NumericPattern)>,
    pub similarity_score: f64,
}

// Helper trait implementations

impl ServiceMatch {
    /// Create a new service match with canonical ordering of service IDs
    pub fn new_canonical(
        service_id_1: ServiceId,
        service_id_2: ServiceId,
        match_method: MatchMethodType,
        confidence_score: f64,
        match_values: MatchValues,
    ) -> Self {
        let (sid1, sid2) = if service_id_1.0 <= service_id_2.0 {
            (service_id_1, service_id_2)
        } else {
            (service_id_2, service_id_1)
        };
        
        Self {
            service_id_1: sid1,
            service_id_2: sid2,
            match_method,
            confidence_score,
            match_values,
        }
    }
    
    /// Get a unique key for this service pair (regardless of order)
    pub fn get_pair_key(&self) -> (ServiceId, ServiceId) {
        if self.service_id_1.0 <= self.service_id_2.0 {
            (self.service_id_1.clone(), self.service_id_2.clone())
        } else {
            (self.service_id_2.clone(), self.service_id_1.clone())
        }
    }
}

impl MethodMatch {
    /// Extract service IDs from match values (implementation depends on MatchValues structure)
    pub fn extract_service_ids(&self) -> Option<(ServiceId, ServiceId)> {
        // This would need to be implemented based on the actual MatchValues structure
        // For now, returning None as a placeholder
        None
    }
}

impl ValidationFlags {
    /// Check if any validation flags indicate issues
    pub fn has_any_issues(&self) -> bool {
        self.incompatible_types || 
        self.conflicting_locations || 
        self.suspicious_patterns
    }
    
    /// Get the most severe issue (for prioritizing rejection reasons)
    pub fn get_primary_issue(&self) -> Option<&'static str> {
        if self.incompatible_types {
            Some("incompatible_service_types")
        } else if self.conflicting_locations {
            Some("conflicting_locations")
        } else if self.suspicious_patterns {
            Some("suspicious_patterns")
        } else {
            None
        }
    }
}

// Extension traits for existing types

/// Extension trait for MatchValues to extract additional information
pub trait MatchValuesExt {
    fn get_url_matching_slugs(&self) -> usize;
    fn get_email_domain(&self) -> Option<String>;
    fn get_normalized_names(&self) -> Option<(String, String)>;
    fn get_embedding_similarity(&self) -> Option<f64>;
}

impl MatchValuesExt for MatchValues {
    fn get_url_matching_slugs(&self) -> usize {
        match self {
            MatchValues::ServiceUrl(url_match) => url_match.matching_slug_count,
            _ => 0,
        }
    }
    
    fn get_email_domain(&self) -> Option<String> {
        match self {
            MatchValues::ServiceEmail(email_match) => {
                email_match.normalized_shared_email
                    .split('@')
                    .nth(1)
                    .map(|s| s.to_string())
            },
            _ => None,
        }
    }
    
    fn get_normalized_names(&self) -> Option<(String, String)> {
        match self {
            MatchValues::ServiceName(name_match) => {
                Some((name_match.normalized_name1.clone(), name_match.normalized_name2.clone()))
            },
            _ => None,
        }
    }
    
    fn get_embedding_similarity(&self) -> Option<f64> {
        match self {
            MatchValues::ServiceEmbedding(embedding_match) => {
                Some(embedding_match.embedding_similarity)
            },
            _ => None,
        }
    }
}

// Utility functions for working with enhanced types

/// Utility functions for service matching
pub mod utils {
    use super::*;
    
    /// Merge multiple method matches for the same service pair
    pub fn merge_method_matches(
        matches: Vec<MethodMatch>,
        weights: &HashMap<MatchMethodType, f64>,
    ) -> Option<ServiceMatch> {
        if matches.is_empty() {
            return None;
        }
        
        // Calculate weighted average confidence
        let mut weighted_confidence = 0.0;
        let mut total_weight = 0.0;
        
        for method_match in &matches {
            let weight = weights.get(&method_match.method).unwrap_or(&1.0);
            weighted_confidence += method_match.confidence * weight;
            total_weight += weight;
        }
        
        let final_confidence = if total_weight > 0.0 {
            weighted_confidence / total_weight
        } else {
            matches[0].confidence
        };
        
        // Use the highest confidence method as primary
        let primary_match = matches
            .iter()
            .max_by(|a, b| a.confidence.partial_cmp(&b.confidence).unwrap_or(std::cmp::Ordering::Equal))?;
        
        // Extract service IDs (this would need proper implementation)
        let (sid1, sid2) = primary_match.extract_service_ids()?;
        
        Some(ServiceMatch::new_canonical(
            sid1,
            sid2,
            primary_match.method.clone(),
            final_confidence,
            primary_match.match_values.clone(),
        ))
    }
    
    /// Calculate compatibility score between two service types
    pub fn calculate_type_compatibility(type1: Option<&str>, type2: Option<&str>) -> ServiceTypeCompatibility {
        match (type1, type2) {
            (Some(t1), Some(t2)) => {
                let are_identical = t1 == t2;
                let are_compatible = are_identical || are_related_service_types(t1, t2);
                let compatibility_score = if are_identical {
                    1.0
                } else if are_compatible {
                    0.7
                } else {
                    0.0
                };
                
                ServiceTypeCompatibility {
                    type_1: Some(t1.to_string()),
                    type_2: Some(t2.to_string()),
                    are_compatible,
                    are_identical,
                    compatibility_score,
                }
            },
            _ => ServiceTypeCompatibility {
                type_1: type1.map(|s| s.to_string()),
                type_2: type2.map(|s| s.to_string()),
                are_compatible: true, // No conflict if one or both are unknown
                are_identical: false,
                compatibility_score: 0.5, // Neutral score
            }
        }
    }
    
    fn are_related_service_types(type1: &str, type2: &str) -> bool {
        let related_groups = [
            &["food_assistance", "nutrition_assistance"][..],
            &["housing", "shelter", "homeless_services"][..],
            &["mental_health", "behavioral_health", "counseling"][..],
            &["healthcare", "medical", "health_services"][..],
            &["legal", "legal_aid", "advocacy"][..],
            &["employment", "job_services", "workforce_development"][..],
            &["childcare", "youth_services", "family_services"][..],
        ];
        
        related_groups.iter().any(|group| {
            group.contains(&type1) && group.contains(&type2)
        })
    }
}