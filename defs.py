from datetime import datetime
from base64 import b64decode
from json import dump as json_dump, load as json_load
from numpy import sqrt, cbrt, mean, abs
from pandas import to_datetime, Timedelta, DataFrame, read_pickle
from sklearn.metrics import mean_absolute_error
from sqlalchemy import create_engine
from our_config import usuario_prod, senha_prod, usuario_teste, senha_teste
from os import path as os_path


def my_mape(y_true, y_pred):
    results = []
    for (y_true_, y_pred_) in zip(y_true, y_pred):
        if y_true_ != 0:
            results.append(abs((y_true_ - y_pred_)/y_true_)*100)
        else:
            results.append(100)
    return mean(results)


this_dir = os_path.dirname(os_path.realpath(__file__))
raw_data_dir = os_path.join(this_dir, 'data/raw')
interim_data_dir = os_path.join(this_dir,'data/interim')
final_data_dir = os_path.join(this_dir,'data/final')
results_dir = os_path.join(this_dir,'results')

    
def depois_do_feriado(row):
    diadasemana = row['ds'].dayofweek
    if (diadasemana < 5) and (diadasemana != 2):
        if diadasemana <= 1:
            somar = 1
        elif diadasemana == 3:
            somar = 4
        elif diadasemana == 4:
            somar = 3
        return row['ds'] + Timedelta(days=somar)
    else:
        return to_datetime("NaT")
    
    
def print_timeseries_metrics(df, name_of_pred_column, register_results=False, modelo='', outros={}):
    df_ = df[['y', name_of_pred_column]].copy()
    df_[name_of_pred_column] = df_[name_of_pred_column].round(0).astype(int)
    df_[f'abs_error_{name_of_pred_column}'] = abs(df_['y']-df_[name_of_pred_column])
    df_[f'ape_{name_of_pred_column}'] = df_[f'abs_error_{name_of_pred_column}']/df_['y']
    this_mape = my_mape(df_['y'], df_[name_of_pred_column])
    this_mae = mean_absolute_error(df_['y'], df_[name_of_pred_column])
    ape90 = df_[f'ape_{name_of_pred_column}'].quantile(0.9)
    ape95 = df_[f'ape_{name_of_pred_column}'].quantile(0.95)
    ae90 =  df_[f'abs_error_{name_of_pred_column}'].quantile(0.9)
    ae95 =  df_[f'abs_error_{name_of_pred_column}'].quantile(0.95)
    
    print(f'Absolute percentage error - Percentil 90%: {ape90: .3%}')
    print(f'Absolute percentage error - Percentil 95%: {ape95: .3%}')
    print(f'Absolute error - Percentil 90%: {ae90: .3f}')
    print(f'Absolute error - Percentil 95%: {ae95: .3f}')
    
    description = df_[[f'abs_error_{name_of_pred_column}',f'ape_{name_of_pred_column}']].describe()
    print(f'Mape: {this_mape :.3%}')
    print(f'Mae: {this_mae :.3f}')
    print(description)
    
    if register_results:
        assert modelo != '', print('O argumento modelo é obrigatório para registrar as métricas')
        assert outros != {}, print('O argumento modelo é obrigatório para registrar as métricas')
        results = {}
        agora = datetime.now().strftime('%H:%M %d/%m/%y')
        results['hora'] = agora
        results['mape'] = this_mape
        results['mae'] = this_mae
        results['ape90'] = ape90
        results['ape95'] = ape95
        results['ae90'] = ae90
        results['ae95'] = ae95
        results['description'] = description.to_dict()
        results['modelo'] = modelo
        results['outros'] = outros
        
        fpath = results_dir+f'/{modelo}.json'
        if os_path.exists(fpath):
            with open(fpath) as file:
                data = json_load(file)
        else:
            data = {}
                
        index = len(data)
        data[index] = results
        
        with open(fpath, "w") as outfile:
            json_dump(data, outfile)
    
        
def print_with_time(txt):
    agora = datetime.now()
    print(f"{agora.strftime('%d/%m/%Y %H:%M:%S')} - {txt}")


def create_db_conn(bd_tns):
    if bd_tns.lower() == 'odi':
        usuario = b64decode(usuario_prod).decode("utf-8")
        senha = b64decode(senha_prod).decode("utf-8")
        vDB_TNS = "DB_ODI_PROD"
    elif bd_tns.lower() == 'test':
        usuario = b64decode(usuario_teste).decode("utf-8")
        senha = b64decode(senha_teste).decode("utf-8")
        vDB_TNS = "DBTESTE1"
    elif bd_tns.lower() == 'prod':
        usuario = b64decode(usuario_teste).decode("utf-8")
        senha = b64decode(senha_teste).decode("utf-8")
        vDB_TNS = "HAOC_TASY_PROD"
    else:
        print("O argumento test_prod precisa ser igual a 'test', 'prod' ou 'odi'")
        return None
    conn = create_engine(f'oracle+cx_oracle://{usuario}:{senha}@{vDB_TNS}')
    return conn


def get_best_args_for_model(model_name, holidays, covariates_series):
    results_fname = model_name.capitalize().replace('_u', 'U')
    fpath = results_dir+f'/{results_fname}.json'
    with open(fpath) as file:
        results = json_load(file)
    results = DataFrame(results).T.sort_values(['mae', 'mape'])
    configs, i = {}, 0
    while 'melhor_coluna' not in list(configs.keys()):
        if 'outros' in list(results.iloc[i].keys()):
            configs = results.iloc[i]['outros']
        i += 1
    ae90 = results.iloc[i-1]['ae90']
    country_holidays = holidays if configs['holidays'] is True else None
    future_covariates = covariates_series[configs['future_covariates']] if configs['future_covariates'] is not None else None
    addSegunda, addTerca, subSabado, subDomingo, addAfterHolidays, subFimAno, melhor_coluna = \
        configs['addSegunda'], configs['addTerca'], configs['subSabado'], configs['subDomingo'], configs['addAfterHolidays'], configs['subFimAno'], configs['melhor_coluna']
    return country_holidays, future_covariates, addSegunda, addTerca, subSabado, subDomingo, addAfterHolidays, subFimAno, melhor_coluna, ae90


def get_ensemble_predicitions_from_column_name(column_name, col_es, col_pr):
    method = [int(s) for s in column_name if s.isdigit()][0]
    if method == 1:
        ensemble = (col_pr + col_es)/2
    elif method == 2:
        ensemble = (col_pr + col_es*2)/3
    elif method == 3:
        ensemble = (col_pr + col_es*3)/4
    elif method == 4:
        ensemble = (col_pr + col_es*4)/5
    elif method == 5:
        ensemble = sqrt(col_pr * col_es)
    elif method == 6:
        ensemble = cbrt(col_pr * (col_es**2))
    elif method == 7:
        ensemble = cbrt((col_pr**2) * col_es)
    elif method == 8:
        ensemble = (col_pr*2 + col_es)/3
    elif method == 9:
        ensemble = (col_pr*3 + col_es)/4
    elif method == 10:
        ensemble = (col_pr*4 + col_es)/5
    else:
        print('Problema ao criar coluna ensemble')
        return None
    return ensemble


def custom_add_and_subtract(df__, addSegunda, addTerca, subSabado, subDomingo, addAfterHolidays, subFimAno):
    df_ = df__.copy()
    df_['weekday'] = df_['ds'].dt.dayofweek
    df_.loc[df_['weekday'] == 0,f'ensemble_preds'] = df_.loc[df_['weekday'] == 0,f'ensemble_preds'] + addSegunda
    df_.loc[df_['weekday'] == 1,f'ensemble_preds'] = df_.loc[df_['weekday'] == 1,f'ensemble_preds'] + addTerca
    df_.loc[df_['after_holidays'] == 1,f'ensemble_preds'] = df_.loc[df_['after_holidays'] == 1, f'ensemble_preds'] + addAfterHolidays
    df_.loc[df_['weekday'] == 5,f'ensemble_preds'] = df_.loc[df_['weekday'] == 5,f'ensemble_preds'] - subSabado
    df_.loc[df_['weekday'] == 6,f'ensemble_preds'] = df_.loc[df_['weekday'] == 6,f'ensemble_preds'] - subDomingo
    df_.loc[df_['fim_ano'] == 1,f'ensemble_preds'] = df_.loc[df_['fim_ano'] == 1, f'ensemble_preds'] - subFimAno
    df_['ensemble_preds'] = df_['ensemble_preds']
    return df_


def create_final_dataset(df_preds, tipo, setor, ae90, especialidade='-'):
    df_ = df_preds.rename(columns={'ds':"dt_previsao", 'ensemble_preds':'qtd_previsao'}).copy()
    df_['cd_estabelecimento'] = 1
    df_['tipo'] = tipo.capitalize()
    df_['ds_classific_setor'] = setor.upper()
    df_['ds_especialidade'] = especialidade
    df_['hrr_previsao'] = '-'
    df_['qtd_previsao_min'] = df_['qtd_previsao'] - ae90
    df_['qtd_previsao_max'] = df_['qtd_previsao'] + ae90
    for col in ['qtd_previsao', 'qtd_previsao_min', 'qtd_previsao_max']:
        df_[col] = df_[col].round(0).astype(int)
    return df_
    
    
def my_mape(y_true, y_pred):
    results = []
    for (y_true_, y_pred_) in zip(y_true, y_pred):
        if y_true_ != 0:
            results.append(abs((y_true_ - y_pred_)/y_true_)*100)
        else:
            results.append(0)
    return mean(results)
    