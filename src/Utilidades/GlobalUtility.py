def obtenerDirectorioGlobal:
    os.path.dirname(__file__) if '__file__' in locals() else os.getcwd()