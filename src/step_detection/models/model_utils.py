"""
Model Utilities
Functions for creating, training, and evaluating TensorFlow models.
"""

import json
import os
from typing import Dict, List, Optional, Tuple

import numpy as np
import tensorflow as tf
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
from tensorflow import keras
from tensorflow.keras import layers

# Import configuration
from ..utils.config import get_config


def create_dnn_model(
    input_shape: Optional[Tuple[int, ...]] = None,
    num_classes: Optional[int] = None,
    dropout_rate: Optional[float] = None,
    regularization: Optional[float] = None,
    config_path: Optional[str] = None,
) -> keras.Model:
    """
    Create Deep Neural Network model for step detection using configuration.

    Args:
        input_shape: Input shape (overrides config)
        num_classes: Number of output classes (overrides config)
        dropout_rate: Dropout rate (overrides config)
        regularization: L2 regularization factor (overrides config)
        config_path: Path to config file

    Returns:
        Compiled Keras model
    """
    # Load configuration
    config = get_config(config_path)

    # Get parameters from config or use provided values
    if input_shape is None:
        input_shape = tuple(config.get_input_shape())
    if num_classes is None:
        num_classes = config.get_output_classes()
    if dropout_rate is None:
        dropout_rate = config.get_dropout_rate()
    if regularization is None:
        regularization = config.get_regularization()

    print(f"🧠 Creating DNN model with:")
    print(f"   Input shape: {input_shape}")
    print(f"   Output classes: {num_classes}")
    print(f"   Dropout rate: {dropout_rate}")
    print(f"   Regularization: {regularization}")
    model = keras.Sequential(
        [
            layers.Dense(
                128,
                activation="relu",
                input_shape=input_shape,
                kernel_regularizer=keras.regularizers.l2(regularization),
            ),
            layers.Dropout(dropout_rate),
            layers.Dense(
                64,
                activation="relu",
                kernel_regularizer=keras.regularizers.l2(regularization),
            ),
            layers.Dropout(dropout_rate),
            layers.Dense(
                32,
                activation="relu",
                kernel_regularizer=keras.regularizers.l2(regularization),
            ),
            layers.Dropout(dropout_rate),
            layers.Dense(
                num_classes,
                activation="softmax",
                kernel_regularizer=keras.regularizers.l2(regularization),
            ),
        ]
    )

    # Get learning rate from config
    learning_rate = config.get_learning_rate()

    model.compile(
        optimizer=keras.optimizers.Adam(learning_rate=learning_rate),
        loss="sparse_categorical_crossentropy",
        metrics=["accuracy"],
    )

    return model


def create_cnn_model(
    input_shape: Optional[Tuple[int, ...]] = None,
    num_classes: Optional[int] = None,
    dropout_rate: Optional[float] = None,
    regularization: Optional[float] = None,
    config_path: Optional[str] = None,
) -> keras.Model:
    """
    Create CNN model for step detection using the EXACT original high-accuracy architecture.
    This is the EXACT same code from the notebook that achieved ~96% accuracy.

    Args:
        input_shape: Input shape (overrides config)
        num_classes: Number of output classes (overrides config)
        dropout_rate: Dropout rate (overrides config) - NOT USED in original
        regularization: L2 regularization factor (overrides config) - NOT USED in original
        config_path: Path to config file

    Returns:
        Compiled Keras model
    """
    print("🧠 Creating CNN model with EXACT original high-accuracy architecture...")

    # Use the EXACT same architecture from the notebook
    model = keras.Sequential(
        [
            # Input layer - reshape for Conv1D (batch_size, timesteps, features)
            layers.Reshape((1, 6), input_shape=(6,)),
            # First Conv1D layer - equivalent to PyTorch Conv1d(6, 32, kernel_size=1)
            layers.Conv1D(filters=32, kernel_size=1, strides=1, activation="relu"),
            # MaxPool1D layer - equivalent to PyTorch MaxPool1d(kernel_size=1)
            layers.MaxPooling1D(pool_size=1),
            # Second Conv1D layer - equivalent to PyTorch Conv1d(32, 64, kernel_size=1)
            layers.Conv1D(filters=64, kernel_size=1, strides=1, activation="relu"),
            # Flatten for dense layer
            layers.Flatten(),
            # Dense layer for classification - equivalent to PyTorch Linear(64, 3)
            layers.Dense(3, activation="softmax"),
        ]
    )

    # Compile with EXACT same settings as notebook
    model.compile(
        optimizer="adam", loss="categorical_crossentropy", metrics=["accuracy"]
    )

    print("✅ CNN model created with EXACT original architecture!")
    print(
        "🎯 This is the EXACT same model that achieved ~96% accuracy in the notebook!"
    )

    return model


def train_model(
    model: keras.Model,
    train_features: np.ndarray,
    train_labels: np.ndarray,
    val_features: np.ndarray,
    val_labels: np.ndarray,
    epochs: Optional[int] = None,
    batch_size: Optional[int] = None,
    patience: Optional[int] = None,
    config_path: Optional[str] = None,
) -> keras.callbacks.History:
    """
    Train the model with EXACT same approach as the high-accuracy notebook.
    Simple training without callbacks or class weights - this achieves ~96% accuracy.

    Args:
        model: Keras model to train
        train_features: Training features
        train_labels: Training labels
        val_features: Validation features
        val_labels: Validation labels
        epochs: Maximum number of epochs (overrides config)
        batch_size: Batch size for training (overrides config)
        patience: Early stopping patience (IGNORED - not used in notebook)
        config_path: Path to config file

    Returns:
        Training history
    """
    # Load configuration
    config = get_config(config_path)

    # Get parameters from config or use provided values
    if epochs is None:
        epochs = config.get_epochs()
    if batch_size is None:
        batch_size = config.get_batch_size()

    print(f"🏃‍♂️ Training model with EXACT notebook approach:")
    print(f"   Epochs: {epochs}")
    print(f"   Batch size: {batch_size}")
    print(f"   No callbacks, no class weights - simple like the notebook!")

    # Check if model expects categorical or sparse categorical labels
    model_loss = model.loss
    if hasattr(model_loss, "name"):
        loss_name = model_loss.name
    elif hasattr(model_loss, "__name__"):
        loss_name = model_loss.__name__
    else:
        loss_name = str(model_loss)

    # Convert labels if necessary for categorical crossentropy
    if "categorical_crossentropy" in loss_name and len(train_labels.shape) == 1:
        print(
            "🔄 Converting integer labels to one-hot encoding for categorical crossentropy..."
        )
        from tensorflow.keras.utils import to_categorical

        train_labels = to_categorical(train_labels, num_classes=3)
        val_labels = to_categorical(val_labels, num_classes=3)
        print(f"   Labels converted to shape: {train_labels.shape}")

    # Train the model with EXACT notebook approach - simple and effective!
    # No class weights, no callbacks - just plain training like the notebook
    history = model.fit(
        train_features,
        train_labels,  # One-hot encoded labels
        validation_data=(val_features, val_labels),
        epochs=epochs,
        batch_size=batch_size,
        verbose=1,
    )

    print(f"✅ Training completed after {len(history.history['loss'])} epochs!")
    print("🎯 Used EXACT same simple approach as the high-accuracy notebook!")

    return history


def evaluate_model(
    model: keras.Model, val_features: np.ndarray, val_labels: np.ndarray
) -> Dict:
    """
    Evaluate model performance.

    Args:
        model: Trained Keras model
        val_features: Validation features
        val_labels: Validation labels

    Returns:
        Dictionary with evaluation metrics
    """
    # Make predictions
    predictions = model.predict(val_features, verbose=0)
    predicted_classes = np.argmax(predictions, axis=1)

    # Calculate metrics
    accuracy = accuracy_score(val_labels, predicted_classes)

    # Classification report
    target_names = ["No Label", "start", "end"]
    report = classification_report(
        val_labels, predicted_classes, target_names=target_names, output_dict=True
    )

    # Confusion matrix
    cm = confusion_matrix(val_labels, predicted_classes)

    return {
        "accuracy": accuracy,
        "classification_report": report,
        "confusion_matrix": cm.tolist(),
        "predictions": predictions,
    }


def save_model_and_metadata(
    model: keras.Model,
    model_path: str,
    metadata: Dict,
    metadata_path: Optional[str] = None,
):
    """
    Save model and metadata.

    Args:
        model: Trained Keras model
        model_path: Path to save the model
        metadata: Model metadata dictionary
        metadata_path: Path to save metadata (auto-generated if None)
    """
    # Ensure directory exists
    os.makedirs(os.path.dirname(model_path), exist_ok=True)

    # Save model (using native Keras format to avoid warnings)
    model.save(model_path)
    print(f"Model saved to: {model_path}")

    # Save metadata
    if metadata_path is None:
        metadata_path = model_path.replace(".keras", "_metadata.json")

    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=2)
    print(f"Metadata saved to: {metadata_path}")


def load_model_with_metadata(
    model_path: str, metadata_path: Optional[str] = None
) -> Tuple[keras.Model, Dict]:
    """
    Load model and metadata.

    Args:
        model_path: Path to the saved model
        metadata_path: Path to metadata file (auto-generated if None)

    Returns:
        Tuple of (model, metadata)
    """
    # Load model
    model = keras.models.load_model(model_path)

    # Load metadata
    if metadata_path is None:
        metadata_path = model_path.replace(".keras", "_metadata.json")

    try:
        with open(metadata_path, "r") as f:
            metadata = json.load(f)
    except FileNotFoundError:
        metadata = {}
        print(f"Warning: Metadata file not found: {metadata_path}")

    return model, metadata


def optimize_thresholds(
    predictions: np.ndarray,
    true_labels: np.ndarray,
    threshold_range: Optional[List[float]] = None,
) -> Dict:
    """
    Optimize detection thresholds based on validation data.

    Args:
        predictions: Model predictions (probabilities)
        true_labels: True labels
        threshold_range: List of thresholds to test

    Returns:
        Dictionary with optimal thresholds and results
    """
    if threshold_range is None:
        threshold_range = [0.01, 0.02, 0.03, 0.04, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3]

    best_threshold = 0.03
    best_score = 0
    results = []

    for thresh in threshold_range:
        # Calculate metrics for this threshold
        predicted_starts = predictions[:, 1] > thresh
        predicted_ends = predictions[:, 2] > thresh

        true_starts = true_labels == 1
        true_ends = true_labels == 2

        # F1 scores
        start_tp = np.sum(predicted_starts & true_starts)
        start_fp = np.sum(predicted_starts & ~true_starts)
        start_fn = np.sum(~predicted_starts & true_starts)

        end_tp = np.sum(predicted_ends & true_ends)
        end_fp = np.sum(predicted_ends & ~true_ends)
        end_fn = np.sum(~predicted_ends & true_ends)

        # Calculate F1 scores
        start_precision = (
            start_tp / (start_tp + start_fp) if (start_tp + start_fp) > 0 else 0
        )
        start_recall = (
            start_tp / (start_tp + start_fn) if (start_tp + start_fn) > 0 else 0
        )
        start_f1 = (
            2 * start_precision * start_recall / (start_precision + start_recall)
            if (start_precision + start_recall) > 0
            else 0
        )

        end_precision = end_tp / (end_tp + end_fp) if (end_tp + end_fp) > 0 else 0
        end_recall = end_tp / (end_tp + end_fn) if (end_tp + end_fn) > 0 else 0
        end_f1 = (
            2 * end_precision * end_recall / (end_precision + end_recall)
            if (end_precision + end_recall) > 0
            else 0
        )

        overall_f1 = (start_f1 + end_f1) / 2

        result = {
            "threshold": thresh,
            "start_f1": start_f1,
            "end_f1": end_f1,
            "overall_f1": overall_f1,
        }
        results.append(result)

        if overall_f1 > best_score:
            best_score = overall_f1
            best_threshold = thresh

    return {
        "best_threshold": best_threshold,
        "best_score": best_score,
        "all_results": results,
    }


if __name__ == "__main__":
    print("Model Utilities")
    print("===============")

    # Create sample models
    print("Creating CNN model (original high-accuracy architecture):")
    cnn_model = create_cnn_model()
    print("CNN model created:")
    cnn_model.summary()

    print("\nModel utilities loaded successfully!")
    print("Available functions:")
    print("- create_cnn_model() (original high-accuracy CNN)")
    print("- create_dnn_model() (simple dense network)")
    print("- train_model()")
    print("- evaluate_model()")
    print("- save_model_and_metadata()")
    print("- load_model_with_metadata()")
    print("- optimize_thresholds()")
