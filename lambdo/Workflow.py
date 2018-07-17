__author__="Alexandr Savinov"

import json

from lambdo.resolve import *
from lambdo.transform import *

import logging
log = logging.getLogger('WORKFLOW')


class Workflow:
    """
    The class represents a workflow.
    """

    workflowNo = 0

    def __init__(self, workflow_json: dict):

        self.workflow_json = workflow_json

        self.id = self.workflow_json.get('id', None)
        if self.id is None:
            self.id = "___workflow___" + str(self.workflowNo)
            self.workflow_json['id'] = self.id
            self.workflowNo += 1

        # Create table objects
        self.tables = self.create_tables()

    def create_tables(self):
        """Create a list of Table objects from json."""
        tables_json = self.workflow_json.get("tables", [])
        tables = [Table(self,x) for x in tables_json]
        return tables

    def get_tables(self, table_names):
        """Find tables with the specified names"""
        tables = filter(lambda x: x.id in table_names, self.tables)
        return tables

    def get_table_number(self, table_name):
        """Find table number in the list"""
        return next(i for i, x in enumerate(self.tables) if x.id == table_name)

    def execute(self):
        """
        Execute the whole workflow.
        This means executing all tables according to their dependencies.
        """
        log.info("Start executing workflow '{0}'.".format(self.id))

        # Execute all tables. Later we will take into account their dependencies.
        for i, tab in enumerate(self.tables):
            tab.execute()

        log.info("Finish executing workflow '{0}'.".format(self.id))


class Table:
    """
    The class represents one table.
    """

    tableNo = 0

    def __init__(self, workflow: Workflow, table_json: dict):

        self.workflow = workflow
        self.table_json = table_json
        self.data = None

        self.id = self.table_json.get('id', None)
        if self.id is None:
            self.id = "___table___" + str(self.tableNo)
            self.table_json['id'] = self.id
            self.tableNo += 1

        # Create column objects
        columns_json = self.table_json.get("columns", [])
        self.columns = self.create_columns()

    def create_columns(self):
        """Create a list of Column objects from json."""
        columns_json = self.table_json.get("columns", [])
        columns = [Column(self,x) for x in columns_json]
        return columns

    def populate(self):
        """
        Populate this table with records.
        """
        return None

    def execute(self):
        """
        Execute the whole table.
        This means populate it, execute all columns and then post-process.
        """
        log.info("===> Start executing table '{0}'".format(self.id))

        # Add records (populate)
        new_data = self.populate()
        if new_data is not None:
            self.data = new_data

        # Add derived columns (evaluate)
        for i, col in enumerate(self.columns):
            col.evaluate()

        #
        # Table row filter
        #

        #
        # Table column filter
        #

        log.info("<=== Finish executing table '{0}'".format(self.id))

class Column:
    """
    The class represents one column definition.
    """

    columnNo = 0

    def __init__(self, table: Table, column_json: dict):

        self.table = table
        self.column_json = column_json

        self.id = self.column_json.get('id', None)
        if self.id is None:
            self.id = "___column___" + str(self.columnNo)
            self.column_json['id'] = self.id
            self.columnNo += 1

    def evaluate(self):
        """
        Evaluate this column.
        """
        log.info("  ===> Start evaluating column '{0}'".format(self.id))

        log.info("  <=== Finish evaluating column '{0}'".format(self.id))


if __name__ == "__main__":
    pass
