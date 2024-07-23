## pyspark and databricks
## Built By Assassin
# Functions Import
from databricks.sdk.runtime import displayHTML
from pyspark.sql.functions import col, lit, when, expr, split, date_format, to_date, \
                                    regexp_replace
# Classes Import
from pyspark.sql.functions import DataFrame, Column

## python
import json, re


# Module Definations

# --------------------------------------------------------------------- #
# --------------------- Common Utility Functions ---------------------- #
# --------------------------------------------------------------------- #

def isPresent(value) -> bool:
    return True if value else False


def isNotPresent(value) -> bool:
    return True if not value else False


def isNotEqual(value1, value2) -> bool:
    return True if value1 != value2 else False


def isEqual(value1, value2) -> bool:
    return True if value1 == value2 else False


def tupplePrettyDisplay(tupple_list,headers=["1","2","3","4","5"]):
    """Display tupple in html table"""
    tr_str = ""
    for val in tupple_list:
        td_str = ""
        for td_1 in range(0,len(val)):
            td_str  += f"<td class='tg'>{val[td_1]}</td>"
        tr_str += f"<tr class='tg'>{td_str}</td>"

    th_str = ""
    for th_1 in range(0,len(headers)):
            th_str  += f"<th class='tg'>{headers[th_1]}</th>"
    css_str ='''
        <style type="text/css">
        .tg  {border:1px solid;padding: 5px;}
        </style>
    '''
    displayHTML(f"{css_str}<table>{th_str}{tr_str}</table>")


# --------------------------------------------------------------------- #
# ---------------------- Common Utility Classes ----------------------- #
# --------------------------------------------------------------------- #


class DataFrameOperations:

    # def __init__(self, dataframe):
    #     self.dataframe = dataframe

    @staticmethod
    def renameColumn(dataframe, old_column, new_column) -> DataFrame:
        return dataframe.withColumnRenamed(old_column, new_column)

    @staticmethod
    def addFixedValueColumn(dataframe, column, value="") -> DataFrame:
        return dataframe.withColumn(column, lit(value))

    @staticmethod
    def deformatNumericData(dataframe, column) -> DataFrame:
        """Keeps only digits and '.' in the column"""
        dataframe = dataframe.withColumn(
            column,
            regexp_replace(f"`{column}`",r"[^0-9.]","")
        )
        return dataframe

    @staticmethod
    def verifyDataframeColumnMapping(dataframe, column_mapping) -> bool:
        flag = True
        dataframe_column_mapping = dict(dataframe.dtypes)
        for column_ in column_mapping:
            given_column = column_[0]
            given_data_type = column_[1]
            if given_column != "":
                try:
                    original_column_datatype = dataframe_column_mapping[given_column]
                    if original_column_datatype != given_data_type:
                        raise Exception(
                            f"Datatype issue - {given_column} - expected: {original_column_datatype}, given: {given_data_type}"
                        )
                except Exception as e:
                    print(f"{e}")
                    flag = False
        return flag

    @staticmethod
    def castDateColumn(dataframe, column, format_tupple):
        """cast column to specific date format according to format provided
                format_tupple = ("yyyy-MM-dd","yyyy-MM-dd")
        https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html """

        current_format = format_tupple[0]
        expected_format = format_tupple[1]
        dataframe = dataframe.withColumn(column, to_date(col(f"`{column}`"),current_format))
        dataframe = dataframe.withColumn(column, date_format(col(f"`{column}`"),expected_format))
        return dataframe


    @staticmethod
    def castColumn(dataframe, column, datatype="string") -> DataFrame:
        # NUMERIC_DATA_TYPES = ["byte","tinyint","short","smallint","int","integer","long","bigint",
        #                       "float","real","double","double precision"]
        if("date" in datatype):
            format_tupple = datatype.get("date")
            return DataFrameOperations.castDateColumn(dataframe, column, format_tupple)
        else:
            return dataframe.withColumn(column, col(f"`{column}`").cast(datatype))


    @staticmethod
    def mapOldColumnsToNewColumns(dataframe, mappings, select_only_new_columns=True) -> DataFrame:
        """
        Maps Old name and datatypes to New using List of Tupple On Dataframe
        Parameters
        ------
        dataframe : Dataframe to be mapped
        mapping   : A list of tupple containing mapping of old and new values for each column
                    # [("Old Column Name", "Old DataType", "New Column Name", "New DataType"),
                    # ("", "", "New Column Name", "New DataType", "New Fixed Value"),
                    # ("", "", "New Column Name", "New DataType")]
        select_only_new_columns    : Weather to keep all columns or only new columns

        Returns
        ------
        Dataframe Containing new column mapping
        ## NOTE: 1) Flat Table Inputs only ##
        """

        if(DataFrameOperations.verifyDataframeColumnMapping(dataframe, mappings) == False):
            raise Exception("Error In Dataframe Mapping")
        # DataFrameOperations.verifyDataframeColumnMapping(dataframe, mappings)
        new_columns = []
        for column_mapping in mappings:
            # column_mapping : (old_column_name, old_column_datatype, new_column_name, new_column_datatype, new_fixed_value)
            old_column_name     = column_mapping[0]
            old_column_datatype = column_mapping[1]
            new_column_name     = column_mapping[2]
            new_column_datatype = column_mapping[3]
            try:
                new_fixed_value = column_mapping[4]
            except IndexError:
                new_fixed_value = None  # function backward compatibility

            if isPresent(old_column_name):
                dataframe = DataFrameOperations.renameColumn(dataframe, old_column_name, new_column_name)
            else:
                dataframe = DataFrameOperations.addFixedValueColumn(dataframe, new_column_name, new_fixed_value)

            if isNotEqual(new_column_datatype, old_column_datatype):
                dataframe = DataFrameOperations.castColumn(dataframe, new_column_name, new_column_datatype)
            new_columns.append(new_column_name)

        if(select_only_new_columns): # backward compatibility
            dataframe = dataframe.select(*new_columns)

        return dataframe


    SUFFIX_POSSITION = 0
    PREFIX_POSSITION = 1
    @staticmethod
    def insertColumnNameKeyword(input_dataframe, keyword, position=SUFFIX_POSSITION, exclude_columns=[]) -> DataFrame:
        """
        Add a keyword string as a suffix(0) or prefix(1) to the name

        Parameters
        ------
        input_dataframe: dataframe to apply operation on
        keyword: keyword to insert
        position: SUFFIX_POSSITION = 0 | PREFIX_POSSITION = 1
        exclude_columns: column(s) to exclude

        Return
        ------
        Dataframe with new column names
        """

        dataframed_mapped = input_dataframe
        input_columns = dataframed_mapped.columns

        for old_column_name in input_columns:
            if(position == 0):
                new_column_name = f"{old_column_name}_{keyword}" #suffix
            elif(position == 1):
                new_column_name = f"{keyword}_{old_column_name}" #prefix
            if(old_column_name not in exclude_columns):
                dataframed_mapped = dataframed_mapped.withColumnRenamed(old_column_name, new_column_name)

        return dataframed_mapped


    @staticmethod
    def deltaFormatedColumns(dataframe, replacer_character="_"):
        '''Replace rejected charters from dataframe column names for delta tables
        " ", "(", ")", "{", "}", ";", "=" '''

        DELTA_REJECTED_CHARACTERS = [" ", "(", ")", "{", "}", ";", "="]
        for old_column_name in dataframe.columns:
            new_column_name = old_column_name
            for character in DELTA_REJECTED_CHARACTERS:
                new_column_name = new_column_name.replace(f"{character}", "_")
            dataframe = dataframe.withColumnRenamed(old_column_name, new_column_name)
        return dataframe
    
    NUMERIC_DATA_TYPES = ["byte","tinyint","short","smallint","int","integer","long","bigint",
                          "float","real","double","double precision"]
    STRING_LIKE_DATA_TYPES = ["string","date","timestamp"]

    @staticmethod
    def getColumnAggregatorJson(dataframe, aggregator, datatypes, columns_to_exclude = []) -> json:
        """
        To get all the valid columns from the given dataframe based on requied datatypes.
        Predefined Datatypes CONSTANTS in Pyspark : NUMERIC_DATA_TYPES, STRING_LIKE_DATA_TYPES

        paramaters:
        -----------
        (required)
        dataframe          : pyspark dataframe to operate
        aggregator         : pyspark aggregartor function name e.g. sum, max, last, etc
        datatypes          : a list containg the required datatypes

        (optional)
        columns_to_exclude : a list containg the column(s) to exclude from the final json
                             Default - []
        return:
        ------
        A json containing column names and datatype pair
        """

        all_columns = dict(dataframe.dtypes)
        required_columns = {}
        for column in all_columns:
            if(all_columns[column] in datatypes) & (column not in columns_to_exclude):
                required_columns.update({column:aggregator})
        return required_columns

    @staticmethod
    def pivotColumns(dataframe, dimensions, metrics = None, preserveColumnNames = True):
        """
        To group all the `metrics` wrt given `dimensions`
        By Default takes `SUM` of NUMERIC_DATA_TYPES
        and `FIRST` of STRING_LIKE_DATA_TYPES
        helper functions: getColumnAggregatorJson

        parameters:
        ----------
        dataframe  : pyspark dataframe to operate
        dimensions : a list containing required dimensions to group metrics on
        metrics    : a json with required column-datatype pair

        (optional)
        preserveColumnNames : Remove aggregate names around original metric names
                              Default : True

        return:
        ------
        A pivot dataframe with all the `metrics` grouped by `dimensions`

        sample usage:
        ------------
        1) Default: 
            df = spark.table("mdh_gold.business_metrics_model")
            PIVOT_COLUMNS = ["account_id", "date"]
            DataFrameOperations.piviotColumns(df, PIVOT_COLUMNS).display()
            
        2) Defined Metrics/Dimensions columns:
            df = spark.table("mdh_gold.business_metrics_model")
            PIVOT_COLUMNS = ["account_id", "date"]
            ALL_OTHER_COLUMNS = DataFrameOperations.getColumnAggregatorJson(
                df, "sum", DataFrameOperations.NUMERIC_DATA_TYPES, PIVOT_COLUMNS
            )
            ALL_OTHER_COLUMNS.update(
                DataFrameOperations.getColumnAggregatorJson(
                    df, "first", DataFrameOperations.STRING_LIKE_DATA_TYPES, PIVOT_COLUMNS
                )
            )
            DataFrameOperations.piviotColumns(df, PIVOT_COLUMNS, ALL_OTHER_COLUMNS).display()
        """
        
        if(not metrics):
            metrics = DataFrameOperations.getColumnAggregatorJson(
            dataframe, "sum", DataFrameOperations.NUMERIC_DATA_TYPES, dimensions
            )
            metrics.update(
                DataFrameOperations.getColumnAggregatorJson(
                    dataframe, "first", DataFrameOperations.STRING_LIKE_DATA_TYPES, dimensions
                )
            )

        grouped_df = dataframe.groupBy(dimensions).agg(metrics)
        
        # - to preserve original column name - #
        if(preserveColumnNames):
            renamed_metrics_cols = [col(f"`{metrics[k]}({k})`").alias(f"{k}") for k in metrics]
            grouped_df = grouped_df.select(dimensions + renamed_metrics_cols)

        return grouped_df


    # @staticmethod
    # def extractFromColumn(column, position, DELIMETER="_") -> Column:
    #     return split(column,DELIMETER)[position]

    # @staticmethod
    # def concatColumn(column, position, DELIMETER="_") -> Column:
    #     return split(column,DELIMETER)[position]


# --------------------------------------------------------------------- #
# ------------------------ MDH Utility Classes ------------------------ #
# --------------------------------------------------------------------- #


class MDHOperations:
    
    CANVAS_BLOB_PATH = "wasbs://canvas@cgfmdwdevblob.blob.core.windows.net/"
    CANVAS_ADLS_PATH = "abfss://canvas@cgfmdwdevadls2.dfs.core.windows.net/"
    BASE_ADLS_PATH = CANVAS_ADLS_PATH + "config/"
    ADLS_SHAREPOINT_FILES = CANVAS_ADLS_PATH + "sharepoint files/"
    ADLS_SHAREPOINT_DATA_FILES = ADLS_SHAREPOINT_FILES + "data/"
    ADLS_SHAREPOINT_CONFIG_FILES = ADLS_SHAREPOINT_FILES + "config/"

    ## -- WIP -- ##
    ## Datorama - lookup , extract, split
    @staticmethod
    def columnLookup(input_dataframe, lookup_dataframe, mapping, checkLookupDuplicates=True) -> DataFrame:
        """
        VLookup a specific column in the lookup_dataframe based on the mapping provided
        Parameters
        ------
        input_dataframe  : Dataframe with the lookup input column(s)
        lookup_dataframe : Dataframe with the lookup output column(s)
        mapping          :  # mapping_example = [
                            #     {
                            #         "output_column": {"name": "Campaign_ID", "datatype": "string"},
                            #         "lookup_column": {
                            #             "name": "Campaign Key",  # Column from right dataframe
                            #             "value_if_null": "AMZ-Orphan",  # AMZ-Orphan_{{orderId}}
                            #         },
                            #         "joining_conditions": {
                            #             "left_columns": [{"name": "creativeName", "split_index": 0, "delimeter": "_"}],
                            #             "right_columns": [{"name": "Media Buy Key", "split_index": None, "delimeter": None}],
                            #             "type": "left_outer",
                            #         },
                            #     }
                            # ]
                            # mapping_example_minified = [
                            #     ("Campaign_ID", "Campaign Key", "AMZ-Orphan", [("creativeName", 0, "_")], [("Media Buy Key", None, None)], "left_outer"),
                            # ]
        Returns
        ------
        Dataframe containing input_columns and new looked up column from lookup_dataframe
        """

        # Check cardinality of the join. Must be many-to-one(input_dataframe-to-lookup_dataframe)
        if(checkLookupDuplicates):
            c1 = lookup_dataframe.count()
            c2 = lookup_dataframe.select(*[right_column.get("name") for right_column in mapping[0].get("joining_conditions").get("right_columns")]).count()
            if(c1 != c2):
                print(f"{lookup_dataframe} : column(s) data not distinct")
                return None

        DELIMITER = "_"
        if mapping[0].get("joining_conditions").get("left_columns").get("index"):
            input_dataframe = input_dataframe.withColumn(
                "left_columns_created",
                split(
                    mapping[0].get("joining_conditions").get("left_columns").get("name"),
                    DELIMITER,
                )[mapping[0].get("joining_conditions").get("left_columns").get("index")],
            )

        joined_dataframe = input_dataframe.join(
            lookup_dataframe,
            input_dataframe["left_columns_created"].eqNullSafe(
                lookup_dataframe[
                    mapping[0].get("joining_conditions").get("right_columns").get("name")
                ],
                mapping[0].get("joining_conditions").get("type"),
            ),
        )

        result_df = joined_dataframe.withColumn(
            mapping[0].get("output_column").get("name"),
            when(
                (lookup_dataframe[mapping[0].get("lookup_column").get("name")].isNull())
                | (not lookup_dataframe[mapping[0].get("lookup_column").get("name")]),
                lookup_dataframe[mapping[0].get("lookup_column").get("value_if_null")],
            ).otherwise(lookup_dataframe[mapping[0].get("lookup_column").get("name")]),
        )

        result_df = result_df.select(
            input_dataframe.columns + [result_df[mapping[0].get("output_column").get("name")]]
        )

        return result_df