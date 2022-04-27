class CustomException(BaseException):
    pass

class WrongFileFormat(CustomException):
    pass

class WrongMetafileException(CustomException):
    pass

class WrongDataFileException(CustomException):
    pass