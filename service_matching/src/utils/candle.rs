use anyhow::{Context, Result as AnyhowResult};
use candle_core::{Device, Tensor};
use once_cell::sync::Lazy;

static CANDLE_DEVICE: Lazy<Device> = Lazy::new(|| {
    // Attempt to initialize Metal device (GPU)
    // Device::new_metal(0) tries to get the first Metal device.
    match Device::new_metal(0) {
        Ok(metal_device) => {
            if metal_device.is_metal() {
                // Double check it's indeed a metal device
                log::info!("Successfully initialized Candle Metal device (GPU).");
                metal_device
            } else {
                // This case should be rare if new_metal(0) succeeded without error
                // and candle-core is built with metal support.
                log::warn!(
                    "Device::new_metal(0) succeeded but device.is_metal() is false. Using CPU."
                );
                Device::Cpu
            }
        }
        Err(err) => {
            // This error occurs if candle-core was not compiled with "metal" feature,
            // or if no Metal device is available/functional.
            log::warn!(
                "Failed to initialize Candle Metal device: {:?}. Falling back to CPU device.",
                err
            );
            log::info!("Using Candle CPU device (with Accelerate if 'accelerate' feature was enabled for candle-core).");
            Device::Cpu
        }
    }
});

pub fn cosine_similarity_candle(v1_slice: &[f32], v2_slice: &[f32]) -> AnyhowResult<f64> {
    if v1_slice.len() != v2_slice.len() {
        return Err(anyhow::anyhow!(
            "Input vector lengths differ: {} vs {}",
            v1_slice.len(),
            v2_slice.len()
        ));
    }
    if v1_slice.is_empty() {
        return Err(anyhow::anyhow!("Input vectors must not be empty"));
    }

    // Transfer data to the selected device when creating tensors.
    // If `device` is a Metal device, the tensor data will reside on the GPU.
    let v1 =
        Tensor::from_slice(v1_slice, (v1_slice.len(),), &CANDLE_DEVICE).with_context(|| {
            format!(
                "Failed to create tensor v1 from slice with len {}",
                v1_slice.len()
            )
        })?;
    let v2 =
        Tensor::from_slice(v2_slice, (v2_slice.len(),), &CANDLE_DEVICE).with_context(|| {
            format!(
                "Failed to create tensor v2 from slice with len {}",
                v2_slice.len()
            )
        })?;

    let dot_product_tensor = ((&v1 * &v2)
        .with_context(|| "Tensor element-wise multiplication for dot product failed")?)
    .sum_all()
    .with_context(|| "Summing tensor for dot product failed")?;
    let dot_product = (dot_product_tensor
        .to_scalar::<f32>() // This might bring data back to CPU if the tensor was on GPU
        .with_context(|| "Converting dot_product tensor to scalar failed")?)
        as f64;

    let mag1_sq_tensor = ((&v1 * &v1)
        .with_context(|| "Tensor element-wise multiplication for v1 magnitude squared failed")?)
    .sum_all()
    .with_context(|| "Summing tensor for v1 magnitude squared failed")?;
    let mag1_tensor = mag1_sq_tensor
        .sqrt()
        .with_context(|| "Sqrt for v1 magnitude failed")?;
    let mag1 = (mag1_tensor
        .to_scalar::<f32>() // This might bring data back to CPU
        .with_context(|| "Converting v1 magnitude tensor to scalar failed")?) as f64;

    let mag2_sq_tensor = ((&v2 * &v2)
        .with_context(|| "Tensor element-wise multiplication for v2 magnitude squared failed")?)
    .sum_all()
    .with_context(|| "Summing tensor for v2 magnitude squared failed")?;
    let mag2_tensor = mag2_sq_tensor
        .sqrt()
        .with_context(|| "Sqrt for v2 magnitude failed")?;
    let mag2 = (mag2_tensor
        .to_scalar::<f32>() // This might bring data back to CPU
        .with_context(|| "Converting v2 magnitude tensor to scalar failed")?) as f64;

    if mag1 == 0.0 || mag2 == 0.0 {
        return Ok(0.0);
    }

    let similarity = dot_product / (mag1 * mag2);

    if similarity.is_nan() || similarity.is_infinite() {
        log::warn!(
            "Calculated similarity is NaN or Infinite. dot_product: {}, mag1: {}, mag2: {}. v1_slice (first 5): {:?}, v2_slice (first 5): {:?}",
            dot_product, mag1, mag2,
            v1_slice.iter().take(5).collect::<Vec<_>>(),
            v2_slice.iter().take(5).collect::<Vec<_>>()
        );
        return Ok(0.0);
    }

    Ok(similarity)
}
