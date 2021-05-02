from metaflow import (
    step,
    FlowSpec,
    Parameter
)
import pandas as pd

class DatasetPreprocessor(FlowSpec):
    dataset_path = Parameter(
        "dataset_path", help="Path to default dataset",
        default='../data/healthcare-dataset-stroke-data.csv'
    )

    @step
    def start(self):
        self.df = pd.read_csv(self.dataset_path)
        self.next(self.ever_married_processing)

    @step
    def ever_married_processing(self):
        self.df['ever_married'] = self.df['ever_married'].map({'Yes': 1, 'No': 0})
        self.next(self.residence_type_processing)
    
    @step
    def residence_type_processing(self):
        self.df['Residence_type'] = self.df['Residence_type'].map({'Urban': 0, 'Rural': 1})
        self.next(self.gender_processing)

    @step
    def gender_processing(self):
        self.df['gender'] = self.df['gender'].map({'Male': 0, 'Female': 1, 'Other': 2})
        self.next(self.work_type_processing)

    @step
    def work_type_processing(self):
        self.df = pd.concat([pd.get_dummies(self.df["work_type"]), self.df.drop("work_type", axis=1)], axis=1)
        self.next(self.smoking_status_processing)

    @step
    def smoking_status_processing(self):
        self.df = pd.concat([pd.get_dummies(self.df["smoking_status"]), self.df.drop("smoking_status", axis=1)], axis=1)
        self.df = self.df.rename(columns= {'Unknown': 'unknown_smoking_status'})
        self.next(self.end)

    @step
    def end(self):
        print(self.df.head(10))
        self.df.to_csv('../data/dataset_preprocessado.csv', index=False)

if __name__ == '__main__':
    DatasetPreprocessor()