from dbcomms import register_movimentacoes_realizadas_dbprod, retrieve_data_from_dbprod, register_movimentacoes_realizadas_dbteste, register_predictions_dbteste, \
    register_movimentacoes_realizadas_dbprod, register_predictions_dbprod
from data_preparation import preprocess_hospital_data_with_filter, preprocess_external_data
from predict import create_predictions
from defs import print_with_time
from datetime import date
from pandas import date_range

    
def ExecuteProgram():
    print()
    retrieve_data_from_dbprod()
    days_to_simulate = date_range('2022-03-23',date.today(),freq='d')
    for day in days_to_simulate:
        print_with_time(f'Simulando dia: {day.strftime("%d/%m/%Y")}')
        preprocess_hospital_data_with_filter(day)
        preprocess_external_data()
        register_movimentacoes_realizadas_dbteste()
        create_predictions()
        register_predictions_dbteste()
        print_with_time('Fim. Sucesso ao executar script')
        print()

    
if __name__ == '__main__':
    ExecuteProgram()
    