import pandas as pd
import numpy as np

class Cleanse:
    """
    merges and cleans the visits and services Dataframes from the 
    Query.get_dataframes() method in ExtractData.py using wrangle() method
    """

    def __init__(self):
        self.D_list = ['DA'] + ['D' + str(i) for i in range(1, 26)]
        self.P_list = ['P' + str(i) for i in range(1, 16)]

    def wrangle(self, visits: pd.DataFrame, services: pd.DataFrame) -> pd.DataFrame:
        """
        makes input DataFrames pulled directly from SQL databases into useful forms

        merges the visits and services DataFrames and then feature engineers binary
        columns representing whether a benign polyp and/or malignant neoplasm was
        discovered, and if a colonoscopy and/or a colectomy was performed

        Parameters: 
            visits (DataFrame): result of SQL query on medical_service_lines
            services (DataFrame): result of SQL query on medical_headers

        Returns:
            data (DataFrame): contains binary features for if a benign and/or malignant
                growth was detected and if a colonoscopy and/or colectomy was performed,
                as well as the total cost of the encounter and numeric ids for the
                encounter, patient, doctor, and hospital
            ids (DataFrame): maps the numeric ids onto the old string ids - a subset of
                data_full
            data_full (DataFrame): like data, but with string ids, not numeric. contains
                duplicate encounters due to the initial merge
            df (DataFrame): a uncleaned table of the merged visits and services
        """
        df = pd.merge(visits, services, how='left', on='encounter_key')

        # drop unnecessary index columns and cardinality=1 columns
        df.drop(columns=['index_x', 'index_y', 'claim_type_code', 'icd_type'], inplace=True)

        # fill in None and empty values with Nan
        df.fillna(value=np.nan, inplace=True)
        for col in list(df.columns):
            df.loc[df[col] == '', col] = np.nan

        # many string columns which should be numeric columns - convert to float columns
        numeric_cols = list(df.columns)
        for col in ['encounter_key', 'patient_id', 'doctor_id', 'hospital_id', 'modifier1', 'modifier2']:
            numeric_cols.remove(col)
        for col in numeric_cols:
            df[col] = df[col].astype(float)

        # feature engineer columns if growth(s) are malignant or benign, and if colonoscopy or colectomy was done
        df = self.feature_engineer(df)

        data = self.clean_data(df)
        # create a copy of the full version of the data
        data_full = data.copy()

        data, ids = self.make_new_ids(data)

        data, ids = self.merge_duplicates(data, ids)

        data = data[['new_encounter_key'] + [col for col in data.columns if col != 'new_encounter_key']]

        return data, ids, data_full, df

    def feature_engineer(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        engineers binary features denoting if diagnosis found any benign
        and/or malignant growth and if any colonoscopy and/or colectomy
        was performed
        """
        # benign polyp if ICD-9-DM is 211.3 or 211.4
        df['benign'] = 0
        for col in self.D_list:
            df.loc[df[col].isin([211.3, 211.4]), 'benign'] = 1

        # colonoscopy if ICD-9-PCS is 45.23 or CPT is 45378, 45380-45385, 45388
        df['colonoscopy'] = 0
        for col in self.P_list:
            df.loc[df[col] == 45.23, 'colonoscopy'] = 1

        colon_codes = ([45378, 44146] + [i for i in range(44150, 44161)] +
                       [i for i in range(44204, 44209)] + [i for i in range(44210, 44213)])
        df.loc[df['procedure'].isin(colon_codes), 'colonoscopy'] = 1

        # malignant growth if diagnosis is 152.*
        df['malignant'] = 0
        for col in self.D_list:
            df.loc[df[col]//1 == 152, 'malignant'] = 1

        # colectomy if P? is 45.8*, 45.7* or procedure is 44110, 44146, 44150-44160; 44204-44208; 44210-44212
        df['colectomy'] = 0
        for col in self.P_list:
            df.loc[((df[col]//0.1)/10).isin([45.8, 45.7]), 'colectomy'] = 1

        colectomy_codes = ([44110, 44146] + [i for i in range(44150, 44161)] +
                           [i for i in range(44204, 44209)] + [i for i in range(44210, 44212)])
        df.loc[df['procedure'].isin(colectomy_codes), 'colectomy'] = 1

        return df

    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        cleans df by creating a new DataFrame data which contains the
        encounter, patient, doctor, and hospital ids for each encounter as well as
        the total amount paid for that encounter
        """
        drop_cols = self.D_list + self.P_list + ['discharge_status_code', 'admit_type_code', 'units', 'procedure'] + \
                    ['revenue_code', 'bill_type_code', 'line_charge', 'diagnosis_group', 'modifier1', 'modifier2']
        data = df.drop(columns=drop_cols)

        data.rename(columns={'total_claim_charge_amount': 'total_claim'}, inplace=True)

        col_order = ['encounter_key', 'benign', 'malignant', 'colonoscopy', 'colectomy', 'total_claim',
                     'doctor_id', 'patient_id', 'hospital_id']
        data = data[col_order]

        return data

    def make_new_ids(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        changes the ids in data to numeric and creates a new DataFrame ids
        to keep track of how new numeric ids match to old ids
        """
        id_cols = ['encounter_key', 'patient_id', 'doctor_id', 'hospital_id']
        new_id_cols = []
        for col in id_cols:
            index = 0
            seen = {}
            new_col = []

            for val in data[col]:
                if val in seen:
                    new_col.append(seen[val])
                else:
                    new_col.append(index)
                    seen[val] = index
                    index += 1

            seen.clear()
            new_id_cols.append('new_' + col)
            data['new_' + col] = new_col

        ids = data[new_id_cols + id_cols]
        data = data[[col for col in data.columns if not (col in id_cols or col in new_id_cols)] + new_id_cols]

        return data, ids

    def merge_duplicates(self, data: pd.DataFrame, ids: pd.DataFrame) -> pd.DataFrame:
        """
        since multiple CPT procedures can be performed during an encounter, data contains
        a small number of encounters for which there are multiple rows, each row denoting
        a different procedure that was performed. I merge these duplicate encounter rows
        by using an OR statement to compare rows of duplicate encounters so no information
        is lost. I then delete the duplicate rows in both data and ids.
        """
        prev_row = None
        prev_key = None
        for row in data.loc[data.duplicated(subset='new_encounter_key', keep=False)].index:
            key = data.loc[row, 'new_encounter_key']

            if key == prev_key:
                for col in ['benign', 'malignant', 'colonoscopy', 'colectomy']:
                    data.loc[row, col] = data.loc[prev_row, col] | data.loc[row, col]

            prev_key = key
            prev_row = row

        data.drop_duplicates(subset='new_encounter_key', keep='last', inplace=True)
        assert data.shape[0] == data['new_encounter_key'].nunique()

        ids.drop_duplicates(subset='new_encounter_key', keep='last', inplace=True)
        assert ids.shape[0] == ids['new_encounter_key'].nunique()

        return data, ids