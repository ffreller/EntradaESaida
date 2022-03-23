from defs import print_with_time, create_db_conn
from sqlalchemy import types as sqlalchemy_types
from sqlalchemy.sql import text as sqlalchemy_text
import pandas as pd
from datetime import datetime

sqlalchemy_dtypes = {'tipo':sqlalchemy_types.VARCHAR(100),
                     'ds_classific_setor':sqlalchemy_types.VARCHAR(100),
                     'ds_especialidade':sqlalchemy_types.VARCHAR(100),
                     'hrr_previsao':sqlalchemy_types.VARCHAR(100),
                     'ds_tipo_atendimento':sqlalchemy_types.VARCHAR(100),
                     'ds_carater_internacao':sqlalchemy_types.VARCHAR(100),
                     'hrr_realizado': sqlalchemy_types.VARCHAR(100)}
   
    
def retrieve_data_from_dbprod():
    conn = create_db_conn('prod')
    query = f"""select cd_estabelecimento,
       nr_atendimento,
       dt_entrada,
       dt_alta,
       ds_tipo_atendimento,
       ds_carater_internacao,
       dt_entrada_unidade,
       dt_saida_unidade,
       ds_classific_setor
  from (select cd_estabelecimento,
               nr_atendimento,
               dt_entrada_atend dt_entrada,
               dt_alta_atend dt_alta,
               ds_tipo_atendimento,
               ds_carater_internacao_atend ds_carater_internacao,
               FIRST_VALUE(dt_entrada_unidade_dd) OVER(PARTITION BY nr_atendimento, ordem ORDER BY dt_entrada_unidade_dd asc NULLS FIRST) dt_entrada_unidade,
               FIRST_VALUE(dt_saida_unidade_dd) OVER(PARTITION BY nr_atendimento, ordem ORDER BY dt_saida_unidade_dd desc NULLS FIRST) dt_saida_unidade,
               ds_classific_setor
          from (select dat.cd_estabelecimento,
                       dat.nr_atendimento,
                       dat.dt_entrada_atend,
                       dat.dt_alta_atend,
                       dat.ds_tipo_atendimento,
                       dat.ds_carater_internacao_atend,
                       fme.dt_entrada_unidade_dd,
                       fme.dt_saida_unidade_dd,
                       dsa.cd_setor_atendimento,
                       dsa.ds_setor_atendimento,
                       dsa.ds_classific_setor,
                       rank() over(partition by dat.nr_atendimento order by fme.dt_entrada_unidade_dd) - rank() over(partition by dat.nr_atendimento, dsa.ds_classific_setor order by fme.dt_entrada_unidade_dd) ordem
                  from dw.fato_paciente_dia fme,
                       dw.dim_setor_atend   dsa,
                       dw.dim_atendimento   dat
                 where 1 = 1
                   and dat.cd_estabelecimento = 1
                   and fme.sksetoratendimento = dsa.sksetoratendimento
                   and fme.skatendimento = dat.skatendimento
                   and trunc(dat.dt_entrada_atend, 'mm') >=
                       to_date('01/01/2017', 'dd/mm/rrrr')
                --and dat.nr_atendimento = 3092291
                 group by dat.cd_estabelecimento,
                          dat.nr_atendimento,
                          dat.dt_entrada_atend,
                          dat.dt_alta_atend,
                          dat.ds_tipo_atendimento,
                          dat.ds_carater_internacao_atend,
                          fme.dt_entrada_unidade_dd,
                          fme.dt_saida_unidade_dd,
                          dsa.cd_setor_atendimento,
                          dsa.ds_setor_atendimento,
                          dsa.ds_classific_setor))
 group by cd_estabelecimento,
          nr_atendimento,
          dt_entrada,
          dt_alta,
          ds_tipo_atendimento,
          ds_carater_internacao,
          dt_entrada_unidade,
          dt_saida_unidade,
          ds_classific_setor
 order by nr_atendimento, dt_entrada_unidade"""
    try:
        print_with_time(f'Começou a baixar dados do DBPROD')
        df = pd.read_sql(query, conn)
    except Exception as e:
        print('Erro ao baixar dados:', e)
        return False
    assert len(df) > 0, f'Erro ao baixar dados do DBPROD'
    print_with_time(f'Dados do DBPROD baixados')
    df.to_pickle('data/raw/query_result.pickle')
    return df


def register_movimentacoes_realizadas_dbteste():
    interim_data_dir = 'data/interim'
    entrada_uti = pd.read_pickle(interim_data_dir+'/entrada_uti.pickle') 
    entrada_ui = pd.read_pickle(interim_data_dir+'/entrada_ui.pickle')
    saida_uti = pd.read_pickle(interim_data_dir+'/saida_uti.pickle')
    saida_ui = pd.read_pickle(interim_data_dir+'/saida_ui.pickle')
    
    agora = (datetime.now() - pd.Timedelta(hours=3))
    for df_ in [entrada_uti, entrada_ui, saida_uti, saida_ui]:
        df_['cd_estabelecimento'] = 1
        df_['dt_carga'] = agora
        df_['ds_especialidade'] = '-'
        df_['hrr_realizado'] = '-'
        df_.rename(columns={'ds':'dt_realizado', 'y':'qtd_realizado'}, inplace=True)
        
    entrada_uti['tipo'] = 'Entrada'
    entrada_ui['tipo'] = 'Entrada'
    saida_uti['tipo'] = 'Saída'
    saida_ui['tipo'] = 'Saída'
    
    entrada_uti['ds_classific_setor'] = 'UTI'
    entrada_ui['ds_classific_setor'] = 'UI'
    saida_uti['ds_classific_setor'] = 'UTI'
    saida_ui['ds_classific_setor'] = 'UI'
        
    final = entrada_uti.append([entrada_ui, saida_uti, saida_ui])
    conn = create_db_conn('test')
    conn.execute(sqlalchemy_text('TRUNCATE TABLE gl_stg_mov_realizado').execution_options(autocommit=True))
    print_with_time('Começou a registrar movimentações realizadas no DBTESTE1')
    final.to_sql(name='gl_stg_mov_realizado', con=conn, if_exists='append', index=False, dtype=sqlalchemy_dtypes, chunksize=1000)
    print_with_time('Movimentações realizadas registradas no DBTESTE1')
    

def register_predictions_dbteste():
    df0 = pd.read_pickle('data/final/previsoes.pickle')
    this_types = df0.dtypes.apply(lambda x: x.name).to_dict()
    colunas_enviadas = list(this_types.keys())
    correct_types = {'cd_estabelecimento': 'int64', 'dt_carga': 'datetime64[ns]', 'tipo': 'object', 'ds_classific_setor': 'object',
                      'ds_especialidade': 'object', 'dt_previsao': 'datetime64[ns]', 'hrr_previsao': 'object', 'qtd_previsao': 'int64',
                      'qtd_previsao_min': 'int64', 'qtd_previsao_max': 'int64'}
    for coluna in correct_types.keys():
        assert coluna in colunas_enviadas, f"A coluna '{coluna}' precisa ser enviada para registro no BD"
        assert this_types[coluna] == correct_types[coluna], f"A coluna '{coluna}' é do tipo {this_types[coluna]}, mas deveria ser {correct_types[coluna]}"
    conn = create_db_conn('test')
    df0.to_sql(name='gl_stg_prev_movimentacao', con=conn, if_exists='append', index=False, dtype=sqlalchemy_dtypes, chunksize=1000)
    print_with_time(f"Predições registradas no DBTESTE1")

    
if __name__ == '__main__':
    # retrieve_data_from_dbprod()
    register_movimentacoes_realizadas_dbteste()
    # register_predictions_dbteste()
    print_with_time('Fim da comunicação com o DW\n')