import great_expectations as gx
import pandas as pd
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def run_checks():
    context = gx.get_context()
    results_summary = []

    datasets = {
        "weather": "factweatherdaily.csv",
        # "taxi": "facttaxidaily.csv",
        "air_quality": "factairqualitydaily.csv",
    }

    expectations = {
        "weather": [
            ("expect_column_values_to_not_be_null", "avg_temp_c"),
            ("expect_column_values_to_not_be_null", "city"),
            ("expect_column_values_to_be_between", "avg_temp_c", -50, 60),
            ("expect_column_values_to_be_between", "avg_humidity", 0),
            ("expect_column_values_to_be_between", "total_precip_mm", 0),
        ],
        # "taxi": [
        #     ("expect_column_values_to_not_be_null", "date_key"),
        #     ("expect_column_values_to_not_be_null", "avg_fare_usd"),
        #     ("expect_column_values_to_be_between", "avg_fare_usd", 0),
        #     ("expect_column_values_to_be_between", "avg_distance_miles", 0),
        # ],
        "air_quality": [
            ("expect_column_values_to_not_be_null", "avg_value"),
            ("expect_column_values_to_not_be_null", "pollutant"),
            ("expect_column_values_to_be_between", "avg_value", 0),
        ],
    }

    for dataset_name, filename in datasets.items():
        path = os.path.join(BASE_DIR, filename)
        
        if not os.path.exists(path):
            results_summary.append(f"\n{dataset_name.upper()}\nFile not found: {filename}")
            continue
            
        df = pd.read_csv(path)

        datasource = context.data_sources.add_pandas(name=dataset_name)
        asset = datasource.add_dataframe_asset(name=f"{dataset_name}_asset")
        batch_definition = asset.add_batch_definition_whole_dataframe(name=f"{dataset_name}_batch_def")
        batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

        passed = 0
        failed = 0
        failures = []

        for check in expectations[dataset_name]:
            check_type = check[0]
            column_name = check[1]

            if check_type == "expect_column_values_to_not_be_null":
                expectation = gx.expectations.ExpectColumnValuesToNotBeNull(column=column_name)
            elif check_type == "expect_column_values_to_be_between":
                expectation = gx.expectations.ExpectColumnValuesToBeBetween(
                    column=column_name, min_value=check[2]
                )
            else:
                continue

            result = batch.validate(expectation)

            if result.success:
                passed += 1
            else:
                failed += 1
                details = result.result
                unexp_count = details.get("unexpected_count", 0)
                unexp_list = details.get("partial_unexpected_list", [])
                
                failures.append(
                    f"  ❌ {check_type} on '{column_name}'\n"
                    f" ↳ Corrupt Records: {unexp_count} rows out of bounds\n"
                    f" ↳ Sample Bad Values: {unexp_list}"
                )

        results_summary.append(
            f"\n=== {dataset_name.upper()} REPORT ===\n"
            f"Passed: {passed} | Failed: {failed}\n"
            + ("\n".join(failures) if failures else "  ✅ All clean!")
        )

    return "\n\n".join(results_summary)


if __name__ == "__main__":
    print(run_checks())