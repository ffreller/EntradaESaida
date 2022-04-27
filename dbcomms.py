from defs import print_with_time, create_db_conn, raw_data_dir, interim_data_dir, final_data_dir
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


# Script para baixar dados de entrada e saída do banco de dados do DB_ODI_PROD
def retrieve_data_from_dbprod():
    # Criando conexão com o BD
    conn = create_db_conn('odi')
    # Query para consultar dados
    query = f"""select cd_estabelecimento,
       nr_atendimento,
       dt_entrada,
       dt_alta,
       ds_tipo_atendimento,
       ds_carater_internacao,
       dt_entrada_unidade,
       dt_saida_unidade,
       ds_classific_setor,
       ds_atend_especialidade ds_especialidade,
       sysdate                dt_carga
  from (select cd_estabelecimento,
               nr_atendimento,
               dt_entrada_atend dt_entrada,
               dt_alta_atend dt_alta,
               ds_tipo_atendimento,
               ds_carater_internacao_atend ds_carater_internacao,
               FIRST_VALUE(dt_entrada_unidade_dd) OVER(PARTITION BY nr_atendimento, ordem ORDER BY dt_entrada_unidade_dd asc NULLS FIRST) dt_entrada_unidade,
               FIRST_VALUE(dt_saida_unidade_dd) OVER(PARTITION BY nr_atendimento, ordem ORDER BY dt_saida_unidade_dd desc NULLS FIRST) dt_saida_unidade,
               ds_classific_setor,
               ds_atend_especialidade
          from (select a.cd_estabelecimento,
                       a.nr_atendimento,
                       a.dt_entrada_atend,
                       a.dt_alta_atend,
                       a.ds_tipo_atendimento,
                       a.ds_carater_internacao_atend,
                       a.dt_entrada_unidade_dd,
                       a.dt_saida_unidade_dd,
                       a.cd_setor_atendimento,
                       a.ds_setor_atendimento,
                       a.ds_atend_especialidade,
                       a.ds_classific_setor,
                       sum(a.valor) over(partition by a.nr_atendimento order by a.dt_entrada_unidade_dd, a.dt_saida_unidade_dd desc nulls last) ordem
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
                               dat.ds_atend_especialidade,
                               dsa.ds_classific_setor,
                               case
                                 when dsa.ds_classific_setor =
                                      LAG(dsa.ds_classific_setor, 1, '#')
                                  OVER(partition by dat.nr_atendimento
                                           ORDER BY fme.dt_entrada_unidade_dd,
                                           fme.dt_saida_unidade_dd desc nulls last) then
                                  0
                                 else
                                  1
                               end valor
                          from dw.fato_paciente_dia fme,
                               dw.dim_setor_atend   dsa,
                               dw.dim_atendimento   dat
                         where 1 = 1
                           and dat.cd_estabelecimento = 1
                           and fme.sksetoratendimento = dsa.sksetoratendimento
                           and fme.skatendimento = dat.skatendimento
						   and dsa.cd_setor_atendimento <> 537
                           and trunc(dat.dt_entrada_atend, 'mm') >=
                               to_date('01/01/2017', 'dd/mm/rrrr')
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
                                  dsa.ds_classific_setor,
                                  dat.ds_atend_especialidade) a))
 where 1 = 1
 group by cd_estabelecimento,
          nr_atendimento,
          dt_entrada,
          dt_alta,
          ds_tipo_atendimento,
          ds_carater_internacao,
          dt_entrada_unidade,
          dt_saida_unidade,
          ds_classific_setor,
          ds_atend_especialidade
 order by nr_atendimento, dt_entrada_unidade"""
    try:
        print_with_time(f'Começou a baixar dados do DB_ODI_PROD')
        #Requisição para baixar os dados
        df = pd.read_sql(query, conn)
    except Exception as e:
        print('Erro ao baixar dados:', e)
        return False
    assert len(df) > 0, print(f'Erro ao baixar dados do DBPROD')
    print_with_time(f'Dados do DB_ODI_PROD baixados')
    # Salvando
    df.to_pickle(raw_data_dir+'/query_result.pickle')
    return True

# Script para registrar entradas e saídas realizadas no DBTESTE1
def register_movimentacoes_realizadas_dbteste():
    # Lendo os datasets que serão registrados
    entrada_uti = pd.read_pickle(interim_data_dir+'/entrada_uti.pickle') 
    entrada_ui = pd.read_pickle(interim_data_dir+'/entrada_ui.pickle')
    saida_uti = pd.read_pickle(interim_data_dir+'/saida_uti.pickle')
    saida_ui = pd.read_pickle(interim_data_dir+'/saida_ui.pickle')
    
    # Adicionando algumas colunas
    agora = datetime.now()
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
    
    # Juntado os datasets
    final = entrada_uti.append([entrada_ui, saida_uti, saida_ui])
    # Criando conexão com o BD
    conn = create_db_conn('test')
    # Rodando comando truncate table
    conn.execute(sqlalchemy_text('TRUNCATE TABLE gl_stg_mov_realizado').execution_options(autocommit=True))
    print_with_time('Começou a registrar movimentações realizadas no DBTESTE1')
    # Enviando os dados
    final.to_sql(name='gl_stg_mov_realizado', con=conn, if_exists='append', index=False, dtype=sqlalchemy_dtypes, chunksize=1000)
    print_with_time('Movimentações realizadas registradas no DBTESTE1')
    

def register_predictions_dbteste():
    # Lendo dataset de previões
    df0 = pd.read_pickle(final_data_dir+'/previsoes.pickle')
    # Comparando os tipos das colunas
    this_types = df0.dtypes.apply(lambda x: x.name).to_dict()
    colunas_enviadas = list(this_types.keys())
    correct_types = {'cd_estabelecimento': 'int64', 'dt_carga': 'datetime64[ns]', 'tipo': 'object', 'ds_classific_setor': 'object',
                      'ds_especialidade': 'object', 'dt_previsao': 'datetime64[ns]', 'hrr_previsao': 'object', 'qtd_previsao': 'int64',
                      'qtd_previsao_min': 'int64', 'qtd_previsao_max': 'int64'}
    # Checando se colunas têm os tipos corretos
    for coluna in correct_types.keys():
        assert coluna in colunas_enviadas, print(f"A coluna '{coluna}' precisa ser enviada para registro no BD")
        assert this_types[coluna] == correct_types[coluna], print(f"A coluna '{coluna}' é do tipo {this_types[coluna]}, mas deveria ser {correct_types[coluna]}")
    # Criando conexão com o BD
    conn = create_db_conn('test')
    # Enviando os dados
    df0.to_sql(name='gl_stg_prev_movimentacao', con=conn, if_exists='append', index=False, dtype=sqlalchemy_dtypes, chunksize=1000)
    print_with_time(f"Predições registradas no DBTESTE1")
    
    
if __name__ == '__main__':
    # retrieve_data_from_dbprod()
    register_movimentacoes_realizadas_dbteste()
    # register_predictions_dbteste()
    print_with_time('Fim da comunicação com o DW\n')