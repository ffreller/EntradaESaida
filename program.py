from dbcomms import retrieve_data_from_dbprod, register_movimentacoes_realizadas_dbteste, register_predictions_dbteste
from data_preparation import preprocess_hospital_data, preprocess_external_data
from predict import create_predictions, daily_prediction_to_period_prediction
from defs import print_with_time

    
def ExecuteProgram():
    print()
    successful_download = retrieve_data_from_dbprod()
    if successful_download:
        # Processar dados do hospital
        preprocess_hospital_data()
        # Processar dados externos
        preprocess_external_data()
        # Registrar movimentações de entrada e saída no DBTESTE1
        register_movimentacoes_realizadas_dbteste('bkp_gl_stg_mov_realizado_0425')
        # Criar as predições
        create_predictions()
        # Fazer os cálculos de períodos
        daily_prediction_to_period_prediction()
        # Regitrar previsões no DBTESTE1
        register_predictions_dbteste('bkp_gl_stg_prev_movimenta_0425')
        print_with_time('Fim. Sucesso ao executar script')

    
if __name__ == '__main__':
    ExecuteProgram()
    