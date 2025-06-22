// src/utils/constants.rs

/// Maximum distance in meters for entities to be considered in the "same town or city" for initial filtering.
/// This is a generous range to avoid comparing entities on opposite sides of a state.
pub const MAX_DISTANCE_FOR_SAME_CITY_METERS: f64 = 999_999_999.0; // 50 kilometers