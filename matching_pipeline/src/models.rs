// src/models.rs

use std::collections::{HashMap, HashSet};
use std::fmt::Debug; // Import the Debug trait

use bytes::BytesMut;
use chrono::NaiveDateTime;
use postgres_types::{FromSql, IsNull, ToSql, Type};
use serde::{Deserialize, Serialize};
use std::error::Error;
use uuid::Uuid;
use xgraph::{graph::graph::NodeId, Graph};

//------------------------------------------------------------------------------
// IDENTIFIER TYPES
//------------------------------------------------------------------------------
// Using newtype pattern for type safety to prevent mixing different ID types

/// Strongly typed identifier for Entity records
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EntityId(pub String);

// Implement ToSql for EntityId
impl ToSql for EntityId {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        // Delegate to the implementation for String
        self.0.to_sql(ty, out)
    }

    fn accepts(ty: &Type) -> bool {
        // EntityId can be used anywhere a String can be used
        <String as ToSql>::accepts(ty)
    }

    fn to_sql_checked(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        self.0.to_sql_checked(ty, out)
    }
}

// Implement FromSql for EntityId
impl<'a> FromSql<'a> for EntityId {
    fn from_sql(ty: &Type, raw: &[u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        // Convert raw bytes to String, then wrap in EntityId
        let s = String::from_sql(ty, raw)?;
        Ok(EntityId(s))
    }

    fn accepts(ty: &Type) -> bool {
        // EntityId can be created from any type that String accepts
        <String as FromSql>::accepts(ty)
    }
}

/// Strongly typed identifier for Organization records from HSDS
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OrganizationId(pub String);

/// Strongly typed identifier for EntityGroup records
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EntityGroupId(pub String);

/// Strongly typed identifier for ServiceGroup records
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ServiceGroupId(pub String);

/// Strongly typed identifier for GroupCluster records
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct GroupClusterId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ServiceGroupClusterId(pub String);

/// Strongly typed identifier for Service records from HSDS
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ServiceId(pub String);

//------------------------------------------------------------------------------
// CORE DOMAIN MODELS
//------------------------------------------------------------------------------

/// Represents an organization entity in the federation
///
/// This is the foundation of the grouping system, representing a single
/// organization from any data source in the federation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entity {
    /// Unique identifier for this entity
    pub id: EntityId,

    /// Reference to the original organization in HSDS
    pub organization_id: OrganizationId,

    /// Human-readable name of the entity (typically from organization.name)
    pub name: Option<String>,

    /// When this entity was first created
    pub created_at: NaiveDateTime,

    /// When this entity was last updated
    pub updated_at: NaiveDateTime,

    /// Identifier for the source system in the federation
    pub source_system: Option<String>,

    /// Original ID from the source system
    pub source_id: Option<String>,
}

/// Links an entity to related records in other HSDS tables
///
/// This enables tracking which services, phones, locations, etc.
/// are associated with each entity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntityFeature {
    /// Unique identifier for this feature link
    pub id: String,

    /// The entity this feature belongs to
    pub entity_id: EntityId,

    /// Name of the HSDS table this feature comes from (e.g., "service", "phone")
    pub table_name: String,

    /// ID of the specific record in that table
    pub table_id: String,

    /// When this feature link was created
    pub created_at: NaiveDateTime,
}

/// Represents a group of two entities that have been matched (pairwise).
///
/// Groups are formed when a pair of entities is found to match based on
/// one or more criteria and confirmed by RL scoring.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntityGroup {
    /// Unique identifier for this group
    pub id: EntityGroupId,

    /// First entity in the pair
    pub entity_id_1: EntityId,

    /// Second entity in the pair (ensure entity_id_1 < entity_id_2 for consistency)
    pub entity_id_2: EntityId,

    /// The type of matching used (e.g., phone, email)
    pub method_type: MatchMethodType,

    /// The actual values that matched for this pair (structured by match type)
    pub match_values: Option<MatchValues>,

    /// RL-derived confidence score for the match between entity_id_1 and entity_id_2.
    pub confidence_score: Option<f64>,

    /// Pre-RL confidence score calculated by the matching method before RL adjustment
    pub pre_rl_confidence_score: Option<f64>,

    /// The cluster this group belongs to (null until clustering is performed)
    pub group_cluster_id: Option<GroupClusterId>,

    /// When this group was first created
    pub created_at: NaiveDateTime,

    /// When this group was last updated
    pub updated_at: NaiveDateTime,
}

// GroupEntity struct is removed as per the refactoring plan.
// The relationship is now directly entity_id_1 and entity_id_2 in EntityGroup.

/// Enum for supported matching method types
///
/// Defines the standardized method types used for entity matching
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum MatchMethodType {
    /// Matching based on normalized website domains
    Url,

    /// Matching based on normalized phone numbers
    Phone,

    /// Matching based on normalized email addresses
    Email,

    /// Matching based on physical address similarity
    Address,

    /// Matching based on geographic proximity
    Geospatial,

    /// Matching based on organization name similarity
    Name,

    /// Custom matcher type (for extensibility)
    Custom(String),

    ServiceNameSimilarity,
    ServiceUrlMatch,
    ServiceEmailMatch,
    ServiceEmbeddingSimilarity,
}

impl MatchMethodType {
    /// Converts the enum to a string representation
    pub fn as_str(&self) -> &str {
        match self {
            Self::Url => "url",
            Self::Phone => "phone",
            Self::Email => "email",
            Self::Address => "address",
            Self::Geospatial => "geospatial",
            Self::Name => "name",
            Self::Custom(s) => s.as_str(),
            Self::ServiceNameSimilarity => "service_name",
            Self::ServiceUrlMatch => "service_url",
            Self::ServiceEmailMatch => "service_email",
            Self::ServiceEmbeddingSimilarity => "service_embedding",
        }
    }

    /// Creates the enum from a string representation
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "url" => Self::Url,
            "phone" => Self::Phone,
            "email" => Self::Email,
            "address" => Self::Address,
            "geospatial" => Self::Geospatial,
            "name" => Self::Name,
            "service_name" => Self::ServiceNameSimilarity,
            "service_url" => Self::ServiceUrlMatch,
            "service_email" => Self::ServiceEmailMatch,
            "service_embedding" => Self::ServiceEmbeddingSimilarity,
            _ => Self::Custom(s.to_string()),
        }
    }
}

// GroupMethod struct is removed as per the refactoring plan.
// Its essential fields (method_type, match_values, confidence_score)
// are now incorporated into or represented by the EntityGroup struct.

/// Represents a consolidated set of overlapping groups
///
/// Clusters are formed when groups are found to share entities,
/// indicating they represent the same real-world organization.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupCluster {
    /// Unique identifier for this cluster
    pub id: GroupClusterId,

    /// Optional descriptive name
    pub name: Option<String>,

    /// Details about what this cluster represents
    pub description: Option<String>,

    /// When this cluster was first created
    pub created_at: NaiveDateTime,

    /// When this cluster was last updated
    pub updated_at: NaiveDateTime,

    /// Total unique entities across all groups in this cluster
    pub entity_count: i32,

    /// Number of groups in this cluster
    pub group_count: i32,

    /// Average internal pair confidence score for entities within this cluster.
    /// Populated by the verify_clusters step.
    /// Corresponds to the 'average_coherence_score' column in the database.
    pub average_coherence_score: Option<f64>, // New field
}

/// Enum for service match status
///
/// Tracks the lifecycle of a potential service match
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ServiceMatchStatus {
    /// Match has been identified but not reviewed
    Potential,

    /// Match has been reviewed and confirmed as correct
    Confirmed,

    /// Match has been reviewed and rejected as incorrect
    Rejected,
}

impl ServiceMatchStatus {
    /// Converts the enum to a string representation
    pub fn as_str(&self) -> &str {
        match self {
            Self::Potential => "potential",
            Self::Confirmed => "confirmed",
            Self::Rejected => "rejected",
        }
    }

    /// Creates the enum from a string representation
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "confirmed" => Self::Confirmed,
            "rejected" => Self::Rejected,
            _ => Self::Potential,
        }
    }
}

/// Records potential matches between services within a cluster
///
/// Used to identify services that may represent the same real-world service
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceMatch {
    /// Unique identifier for this service match
    pub id: String,

    /// The cluster containing the potentially matching services
    pub group_cluster_id: GroupClusterId,

    /// First service in the potential match
    pub service_id_1: ServiceId,

    /// Second service in the potential match
    pub service_id_2: ServiceId,

    /// Measure of service similarity (0.0-1.0)
    pub similarity_score: f32,

    /// Explanation of why services matched
    pub match_reasons: Option<String>,

    /// When this potential match was created
    pub created_at: NaiveDateTime,

    /// Current status of the match
    pub status: ServiceMatchStatus,

    /// User who reviewed the match (if applicable)
    pub reviewed_by: Option<String>,

    /// When the match was reviewed (if applicable)
    pub reviewed_at: Option<NaiveDateTime>,
}

//------------------------------------------------------------------------------
// MATCH VALUE TYPES
//------------------------------------------------------------------------------

/// Union type for different kinds of match values for a PAIR of entities.
///
/// This is a strongly-typed representation of the JSONB data
/// stored in the entity_group.match_values column.
/// Each variant now holds the specific values related to the matched pair.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "values")]
pub enum MatchValues {
    /// URL matching values for the pair
    Url(UrlMatchValue),

    /// Phone matching values for the pair
    Phone(PhoneMatchValue),

    /// Email matching values for the pair
    Email(EmailMatchValue),

    /// Address matching values for the pair
    Address(AddressMatchValue),

    /// Geospatial matching values for the pair
    Geospatial(GeospatialMatchValue),

    /// Name matching values for the pair
    Name(NameMatchValue),

    /// Generic string matching values (for extensibility)
    /// Kept as Vec<String> if multiple generic attributes can apply to a single pair match.
    /// If it's a single generic value, it would be Generic(String).
    /// The plan was less specific here, so retaining Vec for flexibility.
    Generic(Vec<String>),

    ServiceName(ServiceNameMatchValue),
    ServiceUrl(ServiceUrlMatchValue),
    ServiceEmail(EmailMatchValue), // Reuse EmailMatchValue
    ServiceEmbedding(ServiceEmbeddingMatchValue),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceNameMatchValue {
    pub original_name1: String,
    pub original_name2: String,
    pub normalized_name1: String,
    pub normalized_name2: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceUrlMatchValue {
    pub original_url1: String,
    pub original_url2: String,
    pub normalized_shared_domain: String,
    pub matching_slug_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceEmbeddingMatchValue {
    pub name1: String,
    pub name2: String,
    pub embedding_similarity: f64,
}

/// Represents matched URL values for a pair of entities.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UrlMatchValue {
    /// Original URL from the first entity
    pub original_url1: String,
    /// Original URL from the second entity
    pub original_url2: String,
    /// The normalized shared domain that formed the basis of the match
    pub normalized_shared_domain: String,
    /// The number of slugs that match after the domain
    pub matching_slug_count: usize,
}

/// Represents matched phone number values for a pair of entities.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PhoneMatchValue {
    /// Original phone number from the first entity
    pub original_phone1: String,
    /// Original phone number from the second entity
    pub original_phone2: String,
    /// The normalized shared phone number that formed the basis of the match
    pub normalized_shared_phone: String,
    /// Optional extension for the first entity's phone
    pub extension1: Option<String>,
    /// Optional extension for the second entity's phone
    pub extension2: Option<String>,
}

/// Represents matched email address values for a pair of entities.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmailMatchValue {
    /// Original email from the first entity
    pub original_email1: String,
    /// Original email from the second entity
    pub original_email2: String,
    /// The normalized shared email address that formed the basis of the match
    pub normalized_shared_email: String,
}

/// Represents matched physical address values for a pair of entities.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddressMatchValue {
    /// Original address from the first entity
    pub original_address1: String,
    /// Original address from the second entity
    pub original_address2: String,
    /// The normalized shared address that formed the basis of the match
    pub normalized_shared_address: String,
    /// Optional pairwise match score if applicable before RL scoring
    pub pairwise_match_score: Option<f32>,
}

/// Represents matched geospatial location values for a pair of entities.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeospatialMatchValue {
    /// Latitude of the first entity in the pair.
    pub latitude1: f64,
    /// Longitude of the first entity in the pair.
    pub longitude1: f64,
    /// Latitude of the second entity in the pair.
    pub latitude2: f64,
    /// Longitude of the second entity in the pair.
    pub longitude2: f64,
    /// Distance in meters between the two entities in the pair.
    pub distance: f64,
}

/// Represents matched organization name values for a pair of entities.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NameMatchValue {
    /// Original name from the first entity
    pub original_name1: String,
    /// Original name from the second entity
    pub original_name2: String,
    /// Normalized name for the first entity
    pub normalized_name1: String,
    /// Normalized name for the second entity
    pub normalized_name2: String,
    /// Type of match (e.g., fuzzy, semantic) determined before RL, if any
    pub pre_rl_match_type: Option<String>,
}

/// Struct to hold location information for unprocessed entities
pub struct LocationResults {
    pub locations: Vec<(EntityId, f64, f64)>,
    pub has_postgis: bool,
}

/// Struct to hold information about existing groups (now pairs)
pub struct GroupResults {
    // This struct might need significant changes or removal depending on how existing pairs are queried.
    // For now, keeping its structure but noting it's likely to change in usage.
    /// Map of group_id to (entity_id_1, entity_id_2, method_type, match_values_json_string)
    pub groups: HashMap<String, (EntityId, EntityId, String, Option<String>)>,
}

/// Represents a centroid of a spatial cluster
pub struct Centroid {
    pub latitude: f64,
    pub longitude: f64,
}

/// Represents a spatial cluster found through PostgreSQL
pub struct SpatialCluster {
    pub cluster_id: i32,
    // Changed to EntityId for type safety, assuming conversion from String happens during fetch.
    pub entity_ids: Vec<EntityId>,
    pub centroid: Centroid,
    pub entity_count: i64,
}

/// Represents the result of a matching operation
pub struct MatchResult {
    // These fields will represent counts of PAIRS created, and unique entities participating in those pairs.
    pub groups_created: usize, // Number of new pairwise EntityGroup records
    pub entities_matched: usize, // Number of unique entities in new pairs
    // entities_added and entities_skipped might need re-evaluation in pairwise context.
    // For now, keeping them but their meaning might shift.
    pub entities_added: usize,
    pub entities_skipped: usize,
    pub processed_entities: HashSet<EntityId>,
}

// --- Enums for SuggestedAction ---

#[derive(Debug, Clone, Serialize, Deserialize, ToSql, FromSql)]
#[postgres(name = "action_type_enum")]
pub enum ActionType {
    #[postgres(name = "REVIEW_ENTITY_IN_GROUP")] // This might become REVIEW_PAIR or similar
    ReviewEntityInGroup,
    #[postgres(name = "REVIEW_INTER_GROUP_LINK")]
    // This might become REVIEW_INTER_PAIR_LINK or relate to cluster review
    ReviewInterGroupLink,
    #[postgres(name = "SUGGEST_SPLIT_CLUSTER")]
    SuggestSplitCluster,
    #[postgres(name = "SUGGEST_MERGE_CLUSTERS")]
    SuggestMergeClusters,
}

impl ActionType {
    pub fn as_str(&self) -> &'static str {
        match self {
            ActionType::ReviewEntityInGroup => "REVIEW_ENTITY_IN_GROUP",
            ActionType::ReviewInterGroupLink => "REVIEW_INTER_GROUP_LINK",
            ActionType::SuggestSplitCluster => "SUGGEST_SPLIT_CLUSTER",
            ActionType::SuggestMergeClusters => "SUGGEST_MERGE_CLUSTERS",
        }
    }
}

impl From<&str> for ActionType {
    fn from(s: &str) -> Self {
        match s {
            "REVIEW_ENTITY_IN_GROUP" => ActionType::ReviewEntityInGroup,
            "REVIEW_INTER_GROUP_LINK" => ActionType::ReviewInterGroupLink,
            "SUGGEST_SPLIT_CLUSTER" => ActionType::SuggestSplitCluster,
            "SUGGEST_MERGE_CLUSTERS" => ActionType::SuggestMergeClusters,
            _ => panic!("Invalid ActionType string: {}", s),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSql, FromSql)]
#[postgres(name = "suggestion_status_enum")]
pub enum SuggestionStatus {
    #[postgres(name = "PENDING_REVIEW")]
    PendingReview,
    #[postgres(name = "UNDER_REVIEW")]
    UnderReview,
    #[postgres(name = "ACCEPTED")]
    Accepted,
    #[postgres(name = "REJECTED")]
    Rejected,
    #[postgres(name = "IMPLEMENTED")]
    Implemented,
    #[postgres(name = "DEFERRED")]
    Deferred,
}

impl SuggestionStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            SuggestionStatus::PendingReview => "PENDING_REVIEW",
            SuggestionStatus::UnderReview => "UNDER_REVIEW",
            SuggestionStatus::Accepted => "ACCEPTED",
            SuggestionStatus::Rejected => "REJECTED",
            SuggestionStatus::Implemented => "IMPLEMENTED",
            SuggestionStatus::Deferred => "DEFERRED",
        }
    }
}

impl From<&str> for SuggestionStatus {
    fn from(s: &str) -> Self {
        match s {
            "PENDING_REVIEW" => SuggestionStatus::PendingReview,
            "UNDER_REVIEW" => SuggestionStatus::UnderReview,
            "ACCEPTED" => SuggestionStatus::Accepted,
            "REJECTED" => SuggestionStatus::Rejected,
            "IMPLEMENTED" => SuggestionStatus::Implemented,
            "DEFERRED" => SuggestionStatus::Deferred,
            _ => panic!("Invalid SuggestionStatus string: {}", s),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSql, FromSql)]
#[postgres(name = "priority_enum")]
pub enum Priority {
    Low = 0,
    Medium = 1,
    High = 2,
}

// New struct for inserting a new suggested action
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewSuggestedAction {
    pub pipeline_run_id: Option<String>,
    pub action_type: String,
    pub entity_id: Option<String>,
    pub group_id_1: Option<String>,
    pub group_id_2: Option<String>,
    pub cluster_id: Option<String>,
    pub triggering_confidence: Option<f64>,
    pub details: Option<serde_json::Value>,
    pub reason_code: Option<String>,
    pub reason_message: Option<String>,
    pub priority: i32,
    pub status: String,
    pub reviewer_id: Option<String>,
    pub reviewed_at: Option<NaiveDateTime>,
    pub review_notes: Option<String>,
}

// --- Struct for cluster_formation_edges table ---
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterFormationEdge {
    pub id: String,
    pub pipeline_run_id: String,
    pub source_group_id: String, // EntityGroupId (pair ID)
    pub target_group_id: String, // EntityGroupId (pair ID)
    pub calculated_edge_weight: f64,
    pub contributing_shared_entities: Option<serde_json::Value>,
    pub created_at: NaiveDateTime,
}

// New struct for inserting a new cluster formation edge
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewClusterFormationEdge {
    pub pipeline_run_id: String,
    pub source_group_id: String,
    pub target_group_id: String,
    pub calculated_edge_weight: f64,
    pub contributing_shared_entities: Option<serde_json::Value>,
}

// For the contributing_shared_entities JSONB field in ClusterFormationEdge
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContributingSharedEntityDetail {
    pub entity_id: String,
    pub conf_entity_in_source_group: f64, // Confidence of this entity being in the source pair (might be 1.0 or related to original match)
    pub conf_entity_in_target_group: f64, // Confidence of this entity being in the target pair
    #[serde(rename = "W_z")]
    pub w_z: f64, // Weight contribution of this shared entity to the edge
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContributingSharedServiceDetail {
    pub service_id: String,
    pub conf_service_in_source_group: f64,
    pub conf_service_in_target_group: f64,
    #[serde(rename = "W_z")]
    pub w_z: f64,
}

// This struct, inspired by cluster_visualization.rs, holds the necessary data
// for calculating a consolidated edge weight between two entities.
#[derive(Debug, Clone)]
pub struct EntityGroupRecord {
    pub(crate) id: String,
    pub(crate) method_type: String,
    pub(crate) confidence_score: Option<f64>,
    pub(crate) pre_rl_confidence_score: Option<f64>,
}

// Struct to hold visualization edge data before it's inserted into the database.
#[derive(Debug, Clone)]
pub struct VisualizationEdgeData {
    pub cluster_id: String,
    pub entity_id_1: String,
    pub entity_id_2: String,
    pub edge_weight: f64,
    pub details: serde_json::Value,
    pub pipeline_run_id: String,
}

// A generic mapper to handle conversions between your application's IDs and xgraph's numeric NodeIds.
#[derive(Debug)]
pub struct NodeMapper<T: std::hash::Hash + Eq + Clone> {
    id_to_idx: HashMap<T, NodeId>,
    idx_to_id: HashMap<NodeId, T>,
}

// FIX: Add the `Debug` trait bound to `T` as required by `xgraph`.
impl<T: std::hash::Hash + Eq + Clone + Debug> NodeMapper<T> {
    pub fn new() -> Self {
        NodeMapper {
            id_to_idx: HashMap::new(),
            idx_to_id: HashMap::new(),
        }
    }

    pub fn get_or_add_node(&mut self, graph: &mut Graph<f64, T, ()>, id: T) -> NodeId {
        *self.id_to_idx.entry(id.clone()).or_insert_with(|| {
            let node_idx = graph.add_node(id.clone());
            self.idx_to_id.insert(node_idx, id);
            node_idx
        })
    }

    pub fn get_id(&self, idx: NodeId) -> Option<&T> {
        self.idx_to_id.get(&idx)
    }

    pub fn get_node_count(&self) -> usize {
        self.idx_to_id.len()
    }
}