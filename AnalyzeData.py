import pandas as pd


class Analyze:
    def __init__(self, data, ids):
        self.data = data
        self.ids = ids
        self.benign_data = None
        self.mistakes = None

    def remove_weird_doctor(self):
        mask = self.data['new_doctor_id'] == 234
        self.data = self.data[~mask]

    def get_mistakes(self):
        # get data where doctors could have made a mistake
        benign_data = self.data.loc[(self.data['benign'] == 1) & (self.data['malignant'] == 0)]
        self.benign_data = benign_data.reset_index(drop=True)
        doc_ids = self.benign_data['new_doctor_id'].unique()
        results = []

        # for each doctor that could have made a mistake, check how many chances they had to make a mistake
        # and how many mistakes they made
        for i in doc_ids:
            chances = self.benign_data.loc[self.benign_data['new_doctor_id'] == i]
            mistakes = chances.loc[chances['colectomy'] == 1]

            results.append((i, len(chances), len(mistakes), len(mistakes)/len(chances)*100))

        temp = pd.DataFrame(results, columns = ['new_doctor_id', 'chances', 'mistakes', '% mistakes'])
        mistakes = pd.merge(temp, self.ids[['new_doctor_id', 'doctor_id']].drop_duplicates(), on='new_doctor_id')
        self.mistakes = mistakes.sort_values(by=['% mistakes', 'chances'], ascending = [True, False])
        self.mistakes.reset_index(drop=True, inplace=True)

        return self.mistakes