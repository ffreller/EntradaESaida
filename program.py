from dbcomms import retrieve_data_from_dbprod, register_movimentacoes_realizadas_dbteste, register_predictions_dbteste
from data_preparation import preprocess_hospital_data, preprocess_external_data
from predict import create_predictions, daily_prediction_to_period_prediction
from defs import print_with_time

    
def ExecuteProgram():
    print()
    successful_download = retrieve_data_from_dbprod()
    if successful_download:
        preprocess_hospital_data()
        preprocess_external_data()
        register_movimentacoes_realizadas_dbteste('bkp_gl_stg_mov_realizado_0425')
        create_predictions()
        daily_prediction_to_period_prediction()
        register_predictions_dbteste('bkp_gl_stg_prev_movimenta_0425')
        print_with_time('Fim. Sucesso ao executar script')

    
if __name__ == '__main__':
    ExecuteProgram()
    