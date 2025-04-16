#! /usr/bin/env python3

import argparse
import pandas as pd
from typing import Optional


class Anonymizer:
    def __init__(self):
        args = self.parse_args()
        self.anonymize_data(
            filename=args.file,
            column_change=args.column,
            column_remove=args.remove,
            delimiter=args.delimiter,
            output_file=args.output,
        )

    def anonymize_data(
        self,
        filename: str,
        column_change: str,
        column_remove: Optional[str] = None,
        delimiter: Optional[str] = ";",
        output_file: Optional[str] = None,
    ) -> None:
        df = pd.read_csv(filename, delimiter=delimiter)
        if column_change in df.columns:
            df = self.parse_column(df, column_change)
        else:
            if column_change is not None:
                raise ValueError(f"Column '{column_change}' not found in the CSV file.")

        if column_remove in df.columns:
            df = self.remove_column(data=df, column=column_remove)
        else:
            if column_remove is not None:
                raise ValueError(f"Column '{column_remove}' not found in the CSV file.")

        if output_file is None:
            output_file = filename.replace(".csv", "_anonymized.csv")

        self.safe_csv(data=df, filename=output_file, delimiter=delimiter)

    def parse_column(self, data: pd.DataFrame, column: str) -> pd.DataFrame:
        column_data = data[column]
        data_unique = list(set(column_data))
        data[column] = data[column].apply(
            lambda x: data_unique.index(x) + 1 if x in data_unique else x
        )
        print(f"Anonymized column '{column}' with unique ids.")
        return data

    def remove_column(self, data: pd.DataFrame, column: str) -> pd.DataFrame:
        data[column] = data[column].apply(lambda x: "-")
        print(f"Removed all values from column '{column}'.")
        return data

    def safe_csv(
        self, data: pd.DataFrame, filename: str, delimiter: Optional[str] = ";"
    ) -> None:
        try:
            data.to_csv(filename, index=False, sep=delimiter)
            print(f"Anonymized data saved to {filename}")
        except Exception as e:
            print(f"Error saving file: {e}")

    def parse_args(self) -> argparse.Namespace:
        parser = argparse.ArgumentParser(description="Anonymize contract data.")

        parser.add_argument(
            "-f", "--file", type=str, required=True, help="Path to the CSV file."
        )
        parser.add_argument("-c", "--column", type=str, help="Column to anonymize.")
        parser.add_argument(
            "-r", "--remove", type=str, help="Remove all values from the column."
        )
        parser.add_argument(
            "-d", "--delimiter", type=str, default=";", help="CSV delimiter."
        )
        parser.add_argument(
            "-o", "--output", type=str, help="Output file path for the anonymized data."
        )
        args = parser.parse_args()
        if not (args.column or args.remove):
            parser.error("You must specify either --column or --remove.")
        return args


if __name__ == "__main__":
    Anonymizer()
