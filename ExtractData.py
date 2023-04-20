import sqlite3
import pandas as pd


class Codes:
    """
    Asks for user to input codes for Diagnosis ICD-9-CM, Procedure ICD-9-PCS,
    and Procedure PCT, which can be used in Query() class to extract relevant
    data from medical_headers and medical_service_lines in claims.db database

    Attributes:
        D_codes: Diagnosis ICD-9-CM codes 
        P_codes: Procedure ICD-9-PCS codes
        CPT_codes: Procedure CPT codes
    """
    def __init__(self):
        self.D_codes = self.generate_D_codes()
        self.P_codes = self.generate_P_codes()
        self.CPT_codes = self.generate_CPT_codes()

    def get_codes(self, default):
        """
        sub-function necessary to generate ICD-9 codes, called by other methods
        """
        code_list = []

        while True:
            code = input()
            invalid = False

            # check if using default codes
            if code.lower() == 'default':
                if not code_list:
                    code_list = default
                    code = 'done'
                else:
                    print('You have already entered codes! Default must be entered at the start.')
                    invalid = True

            # check if done writing codes
            if code.lower() == 'done':
                break

            # check if code is invalid
            for digit in code:
                if not (ord('0') <= ord(digit) <= ord('9') or digit == '*' or digit == '.'):
                    print('INVALID ENTRY. Please try again.')
                    invalid = True
                    break
            if invalid:
                continue

            code_list.append(code)

        code_list.sort()

        return code_list

    def generate_D_codes(self):
        """
        creates a list of Diagnosis ICD-DM codes input by the user,
        with an option to use the default codes
        """
        print('Please input all Diagnosis ICD-9-CM codes, one at a time.\
              \n\nIf you wish to select all codes after a decimal, place a * after the final digit.\
              \nFor example, if you wished to include all codes beginning with 271, you would enter 271.*\
              \nIf you wished to include all codes beginning with 271.8, you would input 271.8*\
              \n\nOnce you are finished entering codes, enter "done".\
              \n\nIf you would like to use the default codes of 211.3, 211.4 for benign polyps\
              \nand 152.* for malignant growths, enter "default".')

        # default Diagnosis ICD-9-DM codes
        default = ['211.3', '211.4', '152.*']

        D_codes = self.get_codes(default)

        print(f'\n CODES: Your Diagnosis ICD-9-CM codes are {D_codes}')

        return D_codes

    def generate_P_codes(self):
        """
        creates a list of Diagnosis ICD-DM codes input by the user,
        with an option to use the default codes
        """
        print('\n\nPlease input all Procedure ICD-9-PCS codes, one at a time.\
              \n\nIf you wish to select all codes after a decimal, place a * after the final digit.\
              \nFor example, if you wished to include all codes beginning with 271, you would enter 271.*\
              \nIf you wished to include all codes beginning with 271.8, you would input 271.8*\
              \n\nOnce you are finished entering codes, enter "done".\
              \n\nIf you would like to use the default codes of 45.23 for a colonoscopy\
              \nand 45.7*, 45.8* for a colectomy enter "default".')

        # default Procedure ICD-9-PCS codes
        default = ['45.23', '45.7*', '45.8*']

        P_codes = self.get_codes(default)

        print(f'\n CODES: Your Procedure ICD-9-PCS codes are: {P_codes}')

        return P_codes

    def generate_CPT_codes(self):
        '''
        creates a list of Procedure CPT codes input by the user,
        with an option to use the default codes
        '''
        print('\n\nPlease input all Procedure CPT codes, one at a time.\
              \n\nIf you would like to enter a range of codes, use a dash (-) to separate the first and last code\
              \nFor example: "31400-31415" would enter all integer codes from 31400 to 31415, inclusive.\
              \n\nOnce you are finished entering codes, enter "done".\
              \n\nIf you would like to use the default codes for a colonoscopy or a colectomy, enter "default".\
              \nDefault colonoscopy: 45378, 45380-45385, 45388\
              \nDefault colectomy: 44110, 44146, 44150-44160, 44204-44208, 44210-44212')

        code_list = []

        while True:
                    code = input()
                    invalid = False

                    # check if using default codes
                    if code.lower() == 'default':
                        if not code_list:
                            code_list = (['45378', '45388'] + [str(i) for i in range(45380, 45386)] + 
                                         ['44110', '44146'] + [str(i) for i in range(44150, 44161)] + 
                                         [str(i) for i in range(44204, 44209)] + 
                                         [str(i) for i in range(44210, 44213)] )
                            code = 'done'

                        else:
                            print('You have already entered codes! Default must be entered at the start.')
                            invalid = True

                    # check if done with entering codes
                    if code.lower() == 'done':
                        break

                    # check if code is valid
                    for digit in code:
                        if not (ord('0') <= ord(digit) <= ord('9') or digit == '-'):
                            print('INVALID ENTRY. Please try again.')
                            invalid = True
                            break
                    if invalid:
                        continue

                    # check if adding range of codes
                    if '-' in code:
                        first, last = code.split('-')

                        codes = [str(i) for i in range(int(first), int(last)+1)]
                        code_list += codes

                    else:
                        code_list.append(code)

        print(f'\n CODES: Your CPT codes are: {code_list}')

        return code_list


class Query:
    """
    Constructs a SQL query to return two DataFrames of medical_headers and 
    medical_service_lines for input ICD-9 and CPT codes

    Attributes:
        D_codes: Diagnosis ICD-9-CM codes 
        P_codes: Procedure ICD-9-PCS codes
        CPT_codes: Procedure CPT codes
        query: SQL query to extract data containaing 
            any of the ICD-9 codes from medical_headers
        CPT_query: SQL query to extract data containing
            any of the CPT codes from medical_service_lines
        visits: DataFrame of medical_headers containing D_codes and P_codes
        services: DataFrame of medical_service_lines containing CPT_codes
    """
    def __init__(self, D_codes: list, P_codes: list, CPT_codes: list):
        self.D_codes = D_codes
        self.P_codes = P_codes
        self.CPT_codes = CPT_codes

        self.query = None
        self.CPT_query = None
        self.visits = None
        self.services = None

    def get_dataframes(self) -> pd.DataFrame:
        """
        Grabs SQL query for medical_headers and SQL query for 
        medical_service_lines and returns DataFrames from resulting queries

        Stores queries as attributes so they are easy to check
        """
        self.query = self.get_query()
        self.CPT_query = self.get_CPT_query()

        con = sqlite3.connect('claims.db')

        self.visits = pd.read_sql_query(self.query, con)
        self.services = pd.read_sql_query(self.CPT_query, con)

        return self.visits, self.services

    def get_query(self) -> str:
        """
        Constructs a SQL query to extract all rows from the 
        medical_headers database that contain any ICD-9-DM or ICD-PCS 
        code in the attributes .D_codes and .P_codes, respectively
        """
        # create lists of ICD-9 columns matching medical_headers columns
        D_list = ['DA'] + [('D'+str(i)) for i in range(1, 26)]
        P_list = [('P'+str(i)) for i in range(1, 16)]

        # generate a query even if D_codes or P_codes is empty
        if self.D_codes and self.P_codes:

            query = self.create_query(D_list, self.D_codes)
            query = self.fill_query(D_list, self.D_codes[1:], query)
            query = self.fill_query(P_list, self.P_codes, query)

        elif self.D_codes:

            query = self.create_query(D_list, self.D_codes)
            query = self.fill_query(D_list, self.D_codes[1:], query)

        elif self.P_codes:

            query = self.create_query(P_list, self.P_codes)  
            query = self.fill_query(P_list, self.P_codes[1:], query)
        
        else:
            query = None

        return query

    def create_query(self, col_list: list, codes: list) -> str:
        """
        Creates the SQL query to look for the first ICD-9 code,
        which must be instantiated with a WHERE
        """
        col_str = '(' + ','.join(col_list) + ')'

        code = codes[0]
        
        # the first code might have a wild card at the end
        if '*' in code:
            code = code.replace('*', '%')
            query = f'SELECT * FROM medical_headers' + f'\nWHERE {col_list[0]} LIKE "{code}"'
            for col in col_list[1:]:
                    new_query = f'\nOR {col} LIKE "{code}"'
                    query = query + new_query

        else:
            query = f'SELECT * FROM medical_headers' + f'\nWHERE "{code}" in {col_str}'

        return query

    def fill_query(self, col_list: list, codes: list, query: str) -> str:
        """
        Fills the SQL query with OR statements to look for every ICD-9
        code after the first code
        """
        col_str = '(' + ','.join(col_list) + ')'

        for code in codes:

            if '*' in code:
                code = code.replace('*', '%')
                for col in col_list:
                    new_query = f'\nOR {col} LIKE "{code}"'
                    query = query + new_query

            else:
                new_query = f'\nOR "{code}" IN {col_str}'
                query = query + new_query

        return query

    def get_CPT_query(self) -> str:
        """
        Constructs a SQL query to extract all rows from the medical_service_lines database
        that contain any CPT code in the attribute .CPT_codes
        """
        i = 0
        query = f'SELECT * FROM medical_service_lines \nWHERE procedure = {self.CPT_codes[0]}'
        for code in self.CPT_codes[1:]:
            new_query = f'\nOR procedure = "{code}" '
            query = query + new_query

        return query