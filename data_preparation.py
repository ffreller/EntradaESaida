import pandas as pd
from datetime import datetime
from defs import print_with_time, raw_data_dir, interim_data_dir, depois_do_feriado


def preprocess_hospital_data(dia_max:datetime=None):
    if dia_max:
        dia = dia_max
    else:
        dia = pd.to_datetime(datetime.today().date()) #Hoje
        
    dia -= - pd.Timedelta(1, 'd')
    df0 = pd.read_pickle(raw_data_dir+'/query_result.pickle')
    
    assert df0['dt_entrada_unidade'].max() >= dia, \
        print(f"Erro: não há entradas registradas no dia de ontem ({dia.strftime('%d/%m/%Y')}). Data máxima: {df0['dt_entrada_unidade'].max()}")
    assert df0['dt_saida_unidade'].max() >= dia, \
        print(f"Erro: não há saídas registradas no dia de ontem ({dia.strftime('%d/%m/%Y')}). Data máxima: {df0['dt_saida_unidade'].max()}")
    
    # Tirando pacientes eletivos
    df_ = df0.reset_index().copy()
    first = df_.sort_values('dt_entrada_unidade').groupby('nr_atendimento').first().reset_index()
    first_eletivo_idxs = first[first['ds_carater_internacao'] == 'Eletivo']['index'].unique()
    df_entrada = df_.loc[~df_['index'].isin(first_eletivo_idxs)].copy()
    df_entrada = df_entrada[df0.columns]
    
    df_entrada.set_index('dt_entrada_unidade', inplace=True)
    df_saida = df0.set_index('dt_saida_unidade').copy()

    # Separando UI e UTI
    df_entrada_uti0 = df_entrada[df_entrada['ds_classific_setor'] == 'UTI'].copy()
    df_entrada_ui0 = df_entrada[df_entrada['ds_classific_setor'] == 'Unidades de Internação'].copy()
    df_saida_uti0 = df_saida[df_saida['ds_classific_setor'] == 'UTI'].copy()
    df_saida_ui0 = df_saida[df_saida['ds_classific_setor'] == 'Unidades de Internação'].copy()
    
    for tarefa in ['horario', 'dia']:    
        if tarefa == 'horario':
            # Agrupando por período do dia
            df_entrada_uti = df_entrada_uti0['nr_atendimento'].resample('6H').count().reset_index()
            df_saida_uti = df_saida_uti0['nr_atendimento'].resample('6H').count().reset_index()
            df_entrada_ui = df_entrada_ui0['nr_atendimento'].resample('6H').count().reset_index()
            df_saida_ui = df_saida_ui0['nr_atendimento'].resample('6H').count().reset_index()
            
        else:
            #Agrupando por dia
            df_entrada_uti = df_entrada_uti0['nr_atendimento'].resample('1D').count().reset_index()
            df_saida_uti = df_saida_uti0['nr_atendimento'].resample('1D').count().reset_index()
            df_entrada_ui = df_entrada_ui0['nr_atendimento'].resample('1D').count().reset_index()
            df_saida_ui = df_saida_ui0['nr_atendimento'].resample('1D').count().reset_index()

        # Renomeando as colunas
        for df_1 in [df_entrada_uti, df_saida_uti, df_entrada_ui, df_saida_ui]:
            df_1.columns = ["ds", "y"]
            

        # Filtrando o dataset para que tenha valores até o dia anterior
        df_entrada_uti = df_entrada_uti[df_entrada_uti['ds'] <= dia]
        df_entrada_ui = df_entrada_ui[df_entrada_ui['ds'] <= dia]
        df_saida_uti = df_saida_uti[df_saida_uti['ds'] <= dia]
        df_saida_ui = df_saida_ui[df_saida_ui['ds'] <= dia]
        
        #Lidando com dias vazios    
        for val in df_entrada_ui['ds'].unique():
            if val not in df_entrada_uti['ds'].unique():
                df_entrada_uti = df_entrada_uti.append({'ds':val, 'y':0}, ignore_index=True)
            if val not in df_saida_uti['ds'].unique():
                df_saida_uti = df_saida_uti.append({'ds':val, 'y':0}, ignore_index=True)
            if val not in df_saida_ui['ds'].unique():
                df_saida_ui = df_saida_ui.append({'ds':val, 'y':0}, ignore_index=True)
        
        # Resetando o index
        for df_2 in [df_entrada_uti, df_saida_uti, df_entrada_ui, df_saida_ui]:
            df_2.reset_index(drop=True, inplace=True)
            
        # Salvando arquivos
        df_entrada_uti.sort_values('ds').to_pickle(interim_data_dir+f'/entrada_uti_{tarefa}.pickle')
        df_saida_uti.sort_values('ds').to_pickle(interim_data_dir+f'/saida_uti_{tarefa}.pickle')
        df_entrada_ui.sort_values('ds').to_pickle(interim_data_dir+f'/entrada_ui_{tarefa}.pickle')
        df_saida_ui.sort_values('ds').to_pickle(interim_data_dir+f'/saida_ui_{tarefa}.pickle')
    print_with_time('Processamento dos dados hospitalares realizado')
        
    

def preprocess_external_data():
    df_ = pd.read_pickle(interim_data_dir+'/entrada_ui_dia.pickle')
    last_date = pd.to_datetime(df_['ds'].iloc[-1].date())
    proximos7 = [last_date + pd.Timedelta(i, 'd') for i in range(1,8)]
    
    df_to_holidays = df_.copy()
    for dia in proximos7:
        df_to_holidays = df_to_holidays.append({'ds':dia, 'y':0}, ignore_index=True)
        

    dateparser = lambda dates: [datetime.strptime(d, '%Y-%m-%d') for d in dates]
    holidays = pd.read_csv(raw_data_dir+'/holidays.csv', parse_dates=['0'], date_parser=dateparser)
    holidays.columns = ['ds']
    holidays = df_to_holidays.merge(holidays, how='left', on='ds', suffixes=('', '_y'))
    holidays.drop(['y'], axis=1, inplace=True)

    after_holidays = holidays.copy()
    after_holidays['ds'] = holidays.apply(depois_do_feriado, axis=1)
    after_holidays.dropna(inplace=True)
    after_holidays['holiday'] = 1
    after_holidays.rename(columns={'holiday':'after_holidays'}, inplace=True)

    after_holidays = df_to_holidays.merge(after_holidays, on='ds', how="left", suffixes=('', '_y')).fillna(0)
    after_holidays = after_holidays[after_holidays['ds'].duplicated() == False]
    fim_ano = df_to_holidays.copy()
    fim_ano.loc[(fim_ano['ds'].dt.month == 12) & (fim_ano['ds'].dt.day >= 15), 'fim_ano'] = 1
    fim_ano.loc[(fim_ano['ds'].dt.month == 1) & (fim_ano['ds'].dt.day <= 10), 'fim_ano'] = 1
    fim_ano['fim_ano'].fillna(0, inplace=True)
    fim_ano.drop('y', axis=1, inplace=True)

    holidays['holiday'] = 'name'
    
    holidays.sort_values('ds', inplace=True)
    fim_ano = fim_ano[['ds', 'fim_ano']].sort_values('ds').copy()
    after_holidays = after_holidays[['ds', 'after_holidays']].sort_values('ds').copy()
    
    holidays.to_pickle(interim_data_dir+'/holidays.pickle')
    fim_ano.to_pickle(interim_data_dir+'/fim_ano.pickle')
    after_holidays.to_pickle(interim_data_dir+'/after_holidays.pickle')
    print_with_time('Processamento dos dados externos realizado')
    

if __name__ == '__main__':
    preprocess_hospital_data()
    preprocess_external_data()
    print('Fim do processamento dos dados')
    