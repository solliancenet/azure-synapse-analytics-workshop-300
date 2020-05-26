CREATE EXTERNAL TABLE [wwi_ml].[MLModelExt_#USER_CONTEXT#]
(
[Model] [varbinary](max) NULL
)
WITH
(
LOCATION='/ml/onnx-hex/#USER_CONTEXT#' ,
DATA_SOURCE = ModelStorage ,
FILE_FORMAT = csv ,
REJECT_TYPE = VALUE ,
REJECT_VALUE = 0
)
GO

CREATE TABLE [wwi_ml].[MLModel_#USER_CONTEXT#]
(
	[Id] [int] IDENTITY(1,1) NOT NULL,
	[Model] [varbinary](max) NULL,
	[Description] [varchar](200) NULL
)
WITH
(
	DISTRIBUTION = REPLICATE,
	HEAP
)
GO
