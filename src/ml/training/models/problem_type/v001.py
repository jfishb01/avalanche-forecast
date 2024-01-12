"""Simple first iteration classifier model that uses prior day's avalanche forecast as features for prediction.

Note that this would result in a dangerous feedback loop if ever integrated into a real-world scenario.
"""

from fastapi import FastAPI
from sklearn import tree

from src.utils.loggers import set_console_logger
from src.schemas.feature_sets.avalanche_forecast import NUM_POSSIBLE_PROBLEMS
from src.ml.training.training_helpers import (
    parse_cli,
    get_train_test_split,
    get_train_test_set,
    log_model_to_mlflow,
)
from src.schemas.ml.forecast_model import ForecastModel, ModelTypeEnum


app = FastAPI()


@app.post("/train")
def train(ApiQueryParams) -> None:
    """Train and record evaluation metrics for a rudimentary classifier model for avalanche problem type."""
    train_start_analysis_date = ApiQueryParams.train_start_analysis_date
    train_end_analysis_date = ApiQueryParams.train_end_analysis_date
    test_end_analysis_date = ApiQueryParams.test_end_analysis_date

    for i in range(NUM_POSSIBLE_PROBLEMS):
        target = f"problem_{i}"
        features = ["analysis_day_number", target, f"prevalence_{i}"]
        train_test_df = get_train_test_set(
            ApiQueryParams.feature_db_uri,
            ApiQueryParams.distributors,
            train_start_analysis_date,
            test_end_analysis_date,
            features,
            target,
        )
        X_train, X_test, y_train, y_test = get_train_test_split(
            train_test_df, train_end_analysis_date
        )
        model = tree.DecisionTreeClassifier(random_state=42)
        model = model.fit(X_train[features], y_train["target"])

        model = ForecastModel(
            experiment_name=f"{target}.v001",
            mlflow_uri=ApiQueryParams.mlflow_server_uri,
            predictive_model_type=ModelTypeEnum.CLASSIFIER,
            distributors=ApiQueryParams.distributors,
            target=target,
            trained_model=model,
            features=features,
            X_train=X_train,
            X_test=X_test,
            y_train=y_train,
            y_test=y_test,
        )
        log_model_to_mlflow(model)


def main():
    set_console_logger()
    train(parse_cli())


if __name__ == "__main__":
    main()
