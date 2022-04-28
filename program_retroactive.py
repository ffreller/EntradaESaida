from dbcomms import retrieve_data_from_dbprod, register_movimentacoes_realizadas_dbteste, register_predictions_dbteste
from data_preparation import preprocess_hospital_data, preprocess_external_data
from predict import create_predictions, daily_prediction_to_period_prediction
from defs import print_with_time
from datetime import date
from pandas import date_range, to_datetime

    
def ExecuteProgram(start_date, end_date):
    print()
    retrieve_data_from_dbprod()
    days_to_simulate = date_range(start_date, end_date, freq='d')
    for day in days_to_simulate:
        print('\nSimulando dia', day.strftime("%d/%m/%Y"))
        preprocess_hospital_data(day)
        preprocess_external_data()
        register_movimentacoes_realizadas_dbteste('bkp_gl_stg_mov_realizado_0425')
        create_predictions()
        daily_prediction_to_period_prediction()
        register_predictions_dbteste('bkp_gl_stg_prev_movimenta_0425')
        print_with_time('Fim. Sucesso ao executar script')
        print()

    
if __name__ == '__main__':
    dia_str = '01/03/2022'
    this_start_date = to_datetime(dia_str, format="%d/%m/%Y")
    this_end_date = date.today()
    print('*'*40)
    print(f'Simulando de {dia_str} até {this_end_date.strftime("%d/%m/%Y")}')
    print('*'*40)
    ExecuteProgram(this_start_date, this_end_date)
    