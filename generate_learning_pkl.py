# cleaning data
# In dataset, we should have (Document number, Day Groups, Period , Credit product ,Customer ID number,
# Sex , Age Groups , Customer.Address.City , Customer.Region , Loan Amount ,Work Place ,Profession, Salary amount , Field ,
# Gaurantee , Remaining debt ,family status, family members, fc monthly payment, other monthly payment, other expenses,
# Informal income, Count_all_contracts, central bank scoring, DTI) 

# All the data should extract from "teqibat" files NOT "rejected" file
# We need ONLY rows from "teqibat" files that the payment day for customer is arrived not other days

# The given data are : up-to-date "teqibat" file , up-to-date "AS" file
# The given features : (Document number, Day Groups, Period , Credit product ,Customer ID number,
# Sex , Age Groups , Customer.Address.City , Customer.Region , Loan Amount ,Work Place ,Profession, Salary amount , Field ,
# family status, family members, fc monthly payment, other monthly payment, other expenses,
# Informal income,  central bank scoring, DTI) 

# The features should extract from datasets in program : (Gaurantee , Remaining debt , Count_all_contracts)

# The data should not have any missing values , if there are missing values in data the rows should eliminate

# The data should not have 'partner' = 'TEST MMC' in "teqibat" files and 'Organization' = 'TEST' in "AS" file

# The rows contain 'Days Group' = 'OK' or '1-30' or '31-60' or '61-90' and the 'court' column has value should eliminate

# Change Days Group values for 91+ and Court that has value into 91+mahkame

# Change Days Group values for 91+ and Court that does not have value into 91+OK


import psycopg2
import pandas as pd
from sqlalchemy import create_engine


# connect to database
# The columns name should not have %

def connect_database(user, password, databaseName):
    alchemyEngine = create_engine('postgresql://' + user + ':' + password + '@localhost:5432/' + databaseName)
    dbConnection = alchemyEngine.connect()
    predict1 = pd.read_sql("select * from \"predict1\"", dbConnection)
    teq1 = pd.read_sql("select * from \"teq1\"", dbConnection)
    df1_AS_new = pd.read_sql("select * from \"as\"", dbConnection)
    pd.set_option('display.expand_frame_repr', False)
    dbConnection.close()

    return (predict1, teq1, df1_AS_new)


# cleaning teq1 and AS and predict1 class change

def clean_teq_AS_pred(teq1, df1_AS_new, predict1):
    teq1.drop(teq1.index[teq1.partner == 'TEST MMC'], axis=0, inplace=True)
    df1_AS_new.drop(df1_AS_new.index[df1_AS_new.organization == 'TEST'], axis=0, inplace=True)
    teq1['day_groups'].loc[(teq1['day_groups'] == '91+') & (teq1['court'].notnull())] = '91+mahkame'
    teq1['day_groups'].loc[(teq1['day_groups'] == '91+') & (teq1['court'].isnull())] = '91+OK'
    teq2 = teq1[['document_number', 'day_groups']].copy()
    teq2.rename(columns={'document_number': 'document_number'}, inplace=True)
    predict1.drop('day_groups', axis=1, inplace=True)
    predict1 = pd.merge(predict1, teq2, on=['document_number'])
    return (teq1, df1_AS_new, predict1)


# Drop missing values and outliers
def drop_data(predict1, teq1):
    # Drop rows contain nan values
    predict1 = predict1.dropna()

    # Drop the rows thet contain 'Days Group' = 'OK' or '1-30' or '31-60' or '61-90' and the 'court' column has value
    # OK
    okcourt = teq1[teq1['day_groups'] == 'OK'][teq1['court'].notnull()].copy()
    for i in okcourt['document_number']:
        predict1.drop(predict1.index[predict1['document_number'] == i], axis=0, inplace=True)

    # 1-30
    oneCcourt = teq1[teq1['day_groups'] == '1-30'][teq1['court'].notnull()].copy()
    for i in oneCcourt['document_number']:
        predict1.drop(predict1.index[predict1['document_number'] == i], axis=0, inplace=True)

    # 31-60
    Cshcourt = teq1[teq1['day_groups'] == '31-60'][teq1['court'].notnull()].copy()
    for i in Cshcourt['document_number']:
        predict1.drop(predict1.index[predict1['document_number'] == i], axis=0, inplace=True)

    # 61-90
    shNcourt = teq1[teq1['day_groups'] == '61-90'][teq1['court'].notnull()].copy()
    for i in shNcourt['document_number']:
        predict1.drop(predict1.index[predict1['document_number'] == i], axis=0, inplace=True)

    return (predict1)


# Create Count_all_contracts , Remaining debt ,Gaurantee  features
def create_features(teq1, predict1):
    # teq1.rename(columns = {'Id Number ':'Customer ID number'}, inplace = True)

    Remaining = teq1.loc[:][['remaining_debt', 'customer_id_number']].groupby(
        ['customer_id_number']).sum().reset_index()
    predict1 = pd.merge(predict1, Remaining, on=['customer_id_number'])

    df1_AS_new2 = df1_AS_new[['document_number', 'p_g_z']].copy()
    predict1 = pd.merge(predict1, df1_AS_new2, on=['document_number'])
    predict1['p_g_z'].loc[predict1['p_g_z'].notnull()] = 1
    predict1['p_g_z'].loc[predict1['p_g_z'].isnull()] = 0
    predict1.rename(columns={'p_g_z': 'gaurantee'}, inplace=True)

    cnt_allcontracts_cust = df1_AS_new.loc[:][['customer_id_number', 'organization']].groupby(
        ['customer_id_number']).agg('count').reset_index()
    cnt_allcontracts_cust.rename(columns={'organization': 'count_all_contracts'}, inplace=True)
    predict1 = pd.merge(cnt_allcontracts_cust, predict1, on=['customer_id_number'])

    return (predict1)


# train/ test , SMOTE , k-fold cross validation
from imblearn.pipeline import Pipeline
from imblearn.over_sampling import SMOTE

from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import cross_val_score, GridSearchCV, train_test_split, KFold
from sklearn.preprocessing import LabelEncoder
import pandas as pd
import numpy as np
import sqlalchemy
from sklearn.preprocessing import MinMaxScaler


def learning_process(predict1):
    pre_X = predict1[['count_all_contracts',
                      'period', 'credit_product', 'sex', 'age_groups',
                      'customer_address_city', 'customer_region', 'loan_amount', 'salary_amount', 'field',
                      'family_status',
                      'family_members', 'fc_monthly_payment', 'other_monthly_payment',
                      'other_expenses', 'Informal_income', 'central_bank_scoring', 'dti',
                      'remaining_debt', 'gaurantee']]

    day = predict1['day_groups'].values
    day_encoder = LabelEncoder()
    day = day_encoder.fit_transform(day)
    Day = pd.DataFrame(day, columns=['day_transform'])
    Day['day_groups'] = predict1['day_groups'].values
    engine = sqlalchemy.create_engine('postgresql://Saba:1234padra@localhost:5432/payment')
    con = engine.connect()
    Day.to_sql('day', con, if_exists='replace', index=False)
    con.close()

    pre_y = Day['day_transform'].copy()

    dm_X = pd.get_dummies(pre_X)

    # kf = KFold(n_splits=5, random_state=42, shuffle=False)
    # ('sc',MinMaxScaler()),

    sc = MinMaxScaler()
    X = sc.fit_transform(dm_X)

    # imba_pipeline = make_pipeline(SMOTE(random_state=42), RandomForestClassifier(n_estimators=100, random_state=13))
    # pipe = Pipeline([('smt',SMOTE(random_state=42)),('rf',RandomForestClassifier(max_depth=None, random_state=42))], verbose = True)
    # pipe = Pipeline([('rf',RandomForestClassifier(max_depth=None, random_state=42))], verbose = True)

    X_train, X_test, y_train, y_test = train_test_split(X, pre_y, test_size=0.30, random_state=42,
                                                        stratify=predict1['day_groups'])

    # cross_val_score(imba_pipeline, X_train, y_train, scoring='f1_macro', cv=kf)
    smt = SMOTE(random_state=42)
    Xs, y = smt.fit_resample(X_train, y_train)

    rf = RandomForestClassifier(max_depth=None, random_state=42)

    rf.fit(Xs, y)

    y_test_predict = rf.predict(X_test)

    return (Xs, y_test, y_test_predict, rf, X_test)


# params = {
#     'n_estimators': [100],
#     'max_depth': [100],
#     'random_state': [42]
# }

# new_params = {'randomforestclassifier__' + key: params[key] for key in params}
# grid_imba = GridSearchCV(imba_pipeline, param_grid=new_params, cv=kf, scoring='f1_macro',return_train_score=True)

# grid_imba.fit(X_train, y_train)

# grid_imba.cv_results_['mean_test_score'], grid_imba.cv_results_['mean_train_score']
# #grid_imba.best_score_
# #grid_imba.best_params_
# y_test_predict = grid_imba.best_estimator_.predict(X_test)


from sklearn.metrics import f1_score, roc_auc_score
from sklearn.metrics import classification_report, confusion_matrix


def measures(y_test, y_test_predict):
    avg_f1_score = f1_score(y_test, y_test_predict, average='macro')

    confusion_m = confusion_matrix(y_test, y_test_predict)

    classification_r = classification_report(y_test, y_test_predict)

    return (avg_f1_score, confusion_m, classification_r)


predict1, teq1, df1_AS_new = connect_database('Saba', '1234padra', 'payment3')
teq1, df1_AS_new, predict1 = clean_teq_AS_pred(teq1, df1_AS_new, predict1)
predict1 = drop_data(predict1, teq1)
predict1 = create_features(teq1, predict1)
X_train, y_test, y_test_predict, rf, X_test = learning_process(predict1)

avg_f1_score, confusion_m, classification_r = measures(y_test, y_test_predict)

import pickle
import joblib as jb

filename = 'predict_payment.pkl'
jb.dump(rf, filename)

pre_X = predict1[['count_all_contracts',
                  'period', 'credit_product', 'sex', 'age_groups',
                  'customer_address_city', 'customer_region', 'loan_amount', 'salary_amount', 'field', 'family_status',
                  'family_members', 'fc_monthly_payment', 'other_monthly_payment',
                  'other_expenses', 'Informal_income', 'central_bank_scoring', 'dti',
                  'remaining_debt', 'gaurantee']]

day = predict1['day_groups'].values
day_encoder = LabelEncoder()
day = day_encoder.fit_transform(day)
Day = pd.DataFrame(day, columns=['day_transform'])
Day['day_groups'] = predict1['day_groups'].values

pre_y = Day['day_transform'].copy()

dm_X = pd.get_dummies(pre_X)

sc = MinMaxScaler()
X = sc.fit(dm_X)

import pickle
import joblib as jb

filename = 'scaler.pkl'
jb.dump(sc, filename)

dm_X_columns = dm_X.columns
import pickle
import joblib as jb

filename = 'dm_X_columns.pkl'
jb.dump(dm_X_columns, filename)
