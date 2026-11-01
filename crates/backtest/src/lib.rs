//! Walk-forward evaluation, calibration, and proper scoring rules.

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use football_contracts::{FeatureRecord, MatchOutcome};
use linfa::{Dataset, prelude::Fit};
use linfa_logistic::MultiLogisticRegression;
use ndarray::{Array1, Array2};
use scirs2_core::ndarray as calibration_array;
use serde::{Deserialize, Serialize};
use sklears_calibration::isotonic::IsotonicCalibrator;

#[derive(Debug, Clone, Copy)]
pub struct WalkForwardConfig {
    pub minimum_training_rows: usize,
    pub calibration_rows: usize,
    pub test_rows: usize,
    pub step_rows: usize,
    pub reliability_bins: usize,
}

impl Default for WalkForwardConfig {
    fn default() -> Self {
        Self {
            minimum_training_rows: 50,
            calibration_rows: 30,
            test_rows: 20,
            step_rows: 20,
            reliability_bins: 10,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScoreMetrics {
    pub samples: usize,
    pub rps: f64,
    pub brier: f64,
    pub log_loss: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReliabilityBin {
    pub class_index: usize,
    pub bin_index: usize,
    pub samples: usize,
    pub mean_predicted: f64,
    pub observed_frequency: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FoldReport {
    pub train_rows: usize,
    pub calibration_rows: usize,
    pub test_rows: usize,
    pub train_cutoff: DateTime<Utc>,
    pub calibration_cutoff: DateTime<Utc>,
    pub metrics: ScoreMetrics,
    pub reliability: Vec<ReliabilityBin>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestReport {
    pub protocol: String,
    pub total_rows: usize,
    pub folds: Vec<FoldReport>,
    pub out_of_sample: ScoreMetrics,
}

#[derive(Debug, Clone, Copy)]
struct Standardizer {
    mean: [f64; 8],
    scale: [f64; 8],
}

impl Standardizer {
    fn fit(rows: &[FeatureRecord]) -> Result<Self> {
        if rows.is_empty() {
            bail!("cannot fit feature standardization on an empty training set");
        }
        let mut mean = [0.0; 8];
        let mut scale = [0.0; 8];
        for row in rows {
            for (index, value) in row.model_features().into_iter().enumerate() {
                if !value.is_finite() {
                    bail!("non-finite feature value at match {}", row.source_match_id);
                }
                mean[index] += value;
            }
        }
        for value in &mut mean {
            *value /= rows.len() as f64;
        }
        for row in rows {
            for (index, value) in row.model_features().into_iter().enumerate() {
                scale[index] += (value - mean[index]).powi(2);
            }
        }
        for value in &mut scale {
            *value = (*value / rows.len() as f64).sqrt();
            if *value < 1e-12 {
                *value = 1.0;
            }
        }
        Ok(Self { mean, scale })
    }

    fn transform(&self, rows: &[FeatureRecord]) -> Array2<f64> {
        Array2::from_shape_fn((rows.len(), 8), |(row_index, column)| {
            (rows[row_index].model_features()[column] - self.mean[column]) / self.scale[column]
        })
    }
}

pub fn run_walk_forward(
    rows: &[FeatureRecord],
    config: WalkForwardConfig,
) -> Result<BacktestReport> {
    if config.minimum_training_rows == 0
        || config.calibration_rows == 0
        || config.test_rows == 0
        || config.step_rows == 0
        || config.reliability_bins == 0
    {
        bail!("walk-forward window sizes and reliability bins must be positive");
    }
    let mut ordered = rows.to_vec();
    ordered.sort_by_key(|row| (row.decision_at, row.source_match_id));
    for row in &ordered {
        if row.target_result_available_at <= row.decision_at {
            bail!(
                "match {} has a label available at or before decision time",
                row.source_match_id
            );
        }
    }
    if ordered.len() < config.minimum_training_rows + config.calibration_rows + config.test_rows {
        bail!("not enough rows for one train/calibration/test window");
    }

    let mut folds = Vec::new();
    let mut all_test_probabilities = Vec::new();
    let mut all_test_labels = Vec::new();
    let mut train_end = config.minimum_training_rows;
    while train_end + config.calibration_rows + config.test_rows <= ordered.len() {
        let calibration_end = train_end + config.calibration_rows;
        let test_end = calibration_end + config.test_rows;
        let calibration_start = ordered[train_end].decision_at;
        let test_start = ordered[calibration_end].decision_at;
        let train = ordered[..train_end]
            .iter()
            .filter(|row| row.target_result_available_at < calibration_start)
            .cloned()
            .collect::<Vec<_>>();
        let calibration = ordered[train_end..calibration_end]
            .iter()
            .filter(|row| row.target_result_available_at < test_start)
            .cloned()
            .collect::<Vec<_>>();
        let test = &ordered[calibration_end..test_end];
        ensure_all_classes(&train, "matured training")?;
        ensure_all_classes(&calibration, "matured calibration")?;

        let standardizer = Standardizer::fit(&train)?;
        let train_x = standardizer.transform(&train);
        let train_y = Array1::from_iter(train.iter().map(|row| row.outcome.class_index() as u32));
        let dataset = Dataset::new(train_x, train_y);
        let model = MultiLogisticRegression::default()
            .alpha(0.1)
            .max_iterations(300)
            .fit(&dataset)
            .context("fit multinomial logistic regression")?;

        let calibration_x = standardizer.transform(&calibration);
        let raw_calibration = model.predict_probabilities(&calibration_x);
        let calibrators = fit_one_vs_rest_calibrators(&raw_calibration, &calibration)?;
        let test_x = standardizer.transform(test);
        let raw_test = model.predict_probabilities(&test_x);
        let probabilities = apply_one_vs_rest_calibrators(&raw_test, &calibrators)?;
        let labels = test
            .iter()
            .map(|row| row.outcome.class_index())
            .collect::<Vec<_>>();
        let metrics = score_probabilities(&probabilities, &labels)?;
        let reliability = reliability_bins(&probabilities, &labels, config.reliability_bins);
        folds.push(FoldReport {
            train_rows: train.len(),
            calibration_rows: calibration.len(),
            test_rows: test.len(),
            train_cutoff: train.last().unwrap().decision_at,
            calibration_cutoff: calibration.last().unwrap().decision_at,
            metrics,
            reliability,
        });
        all_test_probabilities.extend(probabilities);
        all_test_labels.extend(labels);
        train_end += config.step_rows;
    }
    if folds.is_empty() {
        bail!("configured walk-forward windows produced no folds");
    }
    let out_of_sample = score_probabilities(&all_test_probabilities, &all_test_labels)?;
    Ok(BacktestReport {
        protocol: "expanding_train_then_disjoint_calibration_then_test".to_owned(),
        total_rows: ordered.len(),
        folds,
        out_of_sample,
    })
}

fn ensure_all_classes(rows: &[FeatureRecord], split: &str) -> Result<()> {
    let classes: std::collections::HashSet<_> =
        rows.iter().map(|row| row.outcome.class_index()).collect();
    if classes.len() != 3 {
        bail!("{split} split must contain home win, draw, and away win classes");
    }
    Ok(())
}

fn fit_one_vs_rest_calibrators(
    raw: &Array2<f64>,
    rows: &[FeatureRecord],
) -> Result<Vec<IsotonicCalibrator>> {
    (0..3)
        .map(|class| {
            let probabilities =
                calibration_array::Array1::from_iter(raw.column(class).iter().copied());
            let targets = calibration_array::Array1::from_iter(
                rows.iter()
                    .map(|row| i32::from(row.outcome.class_index() == class)),
            );
            IsotonicCalibrator::new()
                .fit(&probabilities, &targets)
                .map_err(anyhow::Error::from)
                .with_context(|| format!("fit isotonic calibrator for class {class}"))
        })
        .collect()
}

fn apply_one_vs_rest_calibrators(
    raw: &Array2<f64>,
    calibrators: &[IsotonicCalibrator],
) -> Result<Vec<[f64; 3]>> {
    if calibrators.len() != 3 {
        bail!("expected three one-vs-rest calibrators");
    }
    let mut predictions = Vec::with_capacity(raw.nrows());
    for row in raw.rows() {
        let mut values = [0.0; 3];
        for class in 0..3 {
            let probability = calibration_array::Array1::from_vec(vec![row[class]]);
            values[class] = calibrators[class]
                .predict_proba(&probability)
                .map_err(anyhow::Error::from)?[0]
                .clamp(0.0, 1.0);
        }
        let total: f64 = values.iter().sum();
        if total <= 1e-12 {
            values = [1.0 / 3.0; 3];
        } else {
            for value in &mut values {
                *value /= total;
            }
        }
        predictions.push(values);
    }
    Ok(predictions)
}

fn score_probabilities(probabilities: &[[f64; 3]], labels: &[usize]) -> Result<ScoreMetrics> {
    if probabilities.is_empty() || probabilities.len() != labels.len() {
        bail!("probabilities and labels must have the same non-zero length");
    }
    let mut brier = 0.0;
    let mut rps = 0.0;
    let mut log_loss = 0.0;
    for (prediction, &label) in probabilities.iter().zip(labels) {
        if label >= 3
            || prediction
                .iter()
                .any(|value| !value.is_finite() || *value < 0.0)
        {
            bail!("invalid class label or probability in scoring input");
        }
        for (class, probability) in prediction.iter().enumerate() {
            let observed = f64::from(class == label);
            brier += (probability - observed).powi(2);
        }
        let mut predicted_cumulative = 0.0;
        for (class, probability) in prediction.iter().enumerate().take(2) {
            predicted_cumulative += probability;
            let observed_cumulative = if label <= class { 1.0 } else { 0.0 };
            rps += (predicted_cumulative - observed_cumulative).powi(2) / 2.0;
        }
        log_loss -= prediction[label].max(1e-15).ln();
    }
    let count = probabilities.len() as f64;
    Ok(ScoreMetrics {
        samples: probabilities.len(),
        rps: rps / count,
        brier: brier / count,
        log_loss: log_loss / count,
    })
}

fn reliability_bins(
    probabilities: &[[f64; 3]],
    labels: &[usize],
    bin_count: usize,
) -> Vec<ReliabilityBin> {
    let mut output = Vec::new();
    for class in 0..3 {
        let mut counts = vec![0usize; bin_count];
        let mut predicted = vec![0.0; bin_count];
        let mut observed = vec![0.0; bin_count];
        for (probability, &label) in probabilities.iter().zip(labels) {
            let bin = ((probability[class] * bin_count as f64).floor() as usize).min(bin_count - 1);
            counts[bin] += 1;
            predicted[bin] += probability[class];
            observed[bin] += f64::from(label == class);
        }
        for bin in 0..bin_count {
            if counts[bin] > 0 {
                output.push(ReliabilityBin {
                    class_index: class,
                    bin_index: bin,
                    samples: counts[bin],
                    mean_predicted: predicted[bin] / counts[bin] as f64,
                    observed_frequency: observed[bin] / counts[bin] as f64,
                });
            }
        }
    }
    output
}

pub fn outcome_class(outcome: MatchOutcome) -> usize {
    outcome.class_index()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    #[test]
    fn scoring_rules_match_three_class_definition() {
        let metrics = score_probabilities(&[[0.8, 0.1, 0.1]], &[0]).unwrap();
        assert!((metrics.brier - 0.06).abs() < 1e-12);
        assert!(
            (metrics.rps - 0.025).abs() < 1e-12,
            "RPS was {}",
            metrics.rps
        );
        assert!(metrics.log_loss > 0.0);
    }

    #[test]
    fn reliability_bins_cover_each_class_probability() {
        let bins = reliability_bins(&[[0.8, 0.1, 0.1]], &[0], 5);
        assert_eq!(bins.len(), 3);
        assert!(
            bins.iter()
                .any(|bin| bin.class_index == 0 && bin.bin_index == 4)
        );
        assert!(
            bins.iter()
                .any(|bin| bin.class_index == 1 && bin.bin_index == 0)
        );
    }

    #[test]
    fn rejects_label_available_at_decision_time() {
        let date = DateTime::from_timestamp(1_800_000_000, 0).unwrap();
        let row = FeatureRecord {
            source_match_id: 1,
            competition_code: Some("TST".to_owned()),
            decision_at: date,
            target_result_available_at: date,
            home_team_name: "A".to_owned(),
            away_team_name: "B".to_owned(),
            home_scored_avg_5: 1.0,
            home_conceded_avg_5: 1.0,
            away_scored_avg_5: 1.0,
            away_conceded_avg_5: 1.0,
            rest_days_diff: 0.0,
            elo_diff: 0.0,
            home_prior_matches: 0,
            away_prior_matches: 0,
            outcome: MatchOutcome::Draw,
        };
        let result = run_walk_forward(
            &[row],
            WalkForwardConfig {
                minimum_training_rows: 1,
                calibration_rows: 1,
                test_rows: 1,
                step_rows: 1,
                reliability_bins: 2,
            },
        );
        assert!(result.unwrap_err().to_string().contains("label available"));
    }

    #[test]
    fn runs_expanding_train_calibrate_test_folds() {
        let start = DateTime::from_timestamp(1_800_000_000, 0).unwrap();
        let rows = (0..150)
            .map(|index| {
                let class = index % 3;
                let class_signal = class as f64;
                let decision_at = start + Duration::days(index as i64);
                FeatureRecord {
                    source_match_id: index as u64 + 1,
                    competition_code: Some("TST".to_owned()),
                    decision_at,
                    target_result_available_at: decision_at
                        + Duration::hours(if index == 59 || index == 89 { 25 } else { 4 }),
                    home_team_name: "Home".to_owned(),
                    away_team_name: "Away".to_owned(),
                    home_scored_avg_5: class_signal + (index % 7) as f64 * 0.01,
                    home_conceded_avg_5: class_signal * 0.3,
                    away_scored_avg_5: 2.0 - class_signal * 0.4,
                    away_conceded_avg_5: class_signal * 0.2,
                    rest_days_diff: (index % 5) as f64 - 2.0,
                    elo_diff: (class_signal - 1.0) * 80.0,
                    home_prior_matches: index as u32,
                    away_prior_matches: index as u32,
                    outcome: match class {
                        0 => MatchOutcome::HomeWin,
                        1 => MatchOutcome::Draw,
                        _ => MatchOutcome::AwayWin,
                    },
                }
            })
            .collect::<Vec<_>>();
        let report = run_walk_forward(
            &rows,
            WalkForwardConfig {
                minimum_training_rows: 60,
                calibration_rows: 30,
                test_rows: 30,
                step_rows: 30,
                reliability_bins: 5,
            },
        )
        .unwrap();

        assert_eq!(report.folds.len(), 2);
        assert_eq!(report.out_of_sample.samples, 60);
        assert_eq!(report.folds[0].train_rows, 59);
        assert_eq!(report.folds[0].calibration_rows, 29);
        assert!(report.out_of_sample.rps.is_finite());
        assert!(report.out_of_sample.brier.is_finite());
        assert!(report.folds.iter().all(|fold| !fold.reliability.is_empty()));
    }
}
