from dbcomms import  retrieve_data_from_dbprod, register_movimentacoes_realizadas_dbteste, register_predictions_dbteste
from data_preparation import preprocess_hospital_data, preprocess_external_data
from predict import create_predictions
from defs import print_with_time

    
def ExecuteProgram():
    print()
    successful_download = retrieve_data_from_dbprod()
    # Se dados foram baixados, segue
    if successful_download:
        # Processar dados do hospital
        preprocess_hospital_data()
        # Processar dados externos
        preprocess_external_data()
        # Registrar movimentações de entrada e saída no DBTESTE1
        register_movimentacoes_realizadas_dbteste()
        # Criar as predições
        create_predictions()
        # Regitrar previsões no DBTESTE1
        register_predictions_dbteste()
        print_with_time('Fim. Sucesso ao executar script')

    
if __name__ == '__main__':
    ExecuteProgram()
    