from dbcomms import register_movimentacoes_realizadas_dbprod, retrieve_data_from_dbprod, register_movimentacoes_realizadas_dbteste, register_predictions_dbteste, \
    register_movimentacoes_realizadas_dbprod, register_predictions_dbprod
from data_preparation import preprocess_hospital_data, preprocess_external_data
from predict import create_predictions
from defs import print_with_time

    
def ExecuteProgram():
    print()
    retrieve_data_from_dbprod()
    preprocess_hospital_data()
    preprocess_external_data()
    register_movimentacoes_realizadas_dbteste()
    # register_movimentacoes_realizadas_dbprod()
    create_predictions()
    register_predictions_dbteste()
    # register_predictions_dbprod()
    print_with_time('Fim. Sucesso ao executar script')
    
if __name__ == '__main__':
    ExecuteProgram()
    