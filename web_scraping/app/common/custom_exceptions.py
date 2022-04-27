class CustomException(BaseException):
    pass

class WrongDateException(CustomException):
    pass

class WrongFileFormat(CustomException):
    pass

class WrongMetafileException(CustomException):
    pass

class TagNotFoundException(CustomException):
    pass