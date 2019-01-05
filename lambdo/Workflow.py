__author__="Alexandr Savinov"

import json

from lambdo.utils import *
from lambdo.resolve import *
from lambdo.transform import *

from lambdo.Workflow import *
from lambdo.Table import *
from lambdo.Column import *

import logging
log = logging.getLogger('WORKFLOW')


class Workflow:
    """
    The class represents a workflow.
    It is a list of tables and columns.
    It provides the logic of workflow execution.
    """

    workflowNo = 0

    def __init__(self, workflow_json: dict):

        self.workflow_json = workflow_json

        self.id = self.workflow_json.get('id', None)
        if self.id is None:
            self.id = "___workflow___" + str(self.workflowNo)
            self.workflow_json['id'] = self.id
            self.workflowNo += 1

        #
        # Prepare execution environment
        #
        imports = self.workflow_json.get('imports', [])
        self.modules = import_modules(imports)

        #
        # Create table objects
        #
        self.tables = self.create_tables()

    def create_tables(self):
        """Create a list of Table objects from json."""
        tables_json = self.workflow_json.get("tables", [])
        tables = [Table(self,x) for x in tables_json]
        return tables

    def get_tables(self, table_names):
        """Find tables with the specified names"""
        if not table_names: return None
        tables = filter(lambda x: x.id in table_names, self.tables)
        return list(tables)

    def get_table_number(self, table_name):
        """Find table number in the list"""
        return next(i for i, x in enumerate(self.tables) if x.id == table_name)

    def execute(self):
        """
        Execute the whole workflow.
        This means executing all tables according to their dependencies.
        """
        log.info("Start executing workflow '{0}'.".format(self.id))

        # Currently we populate all tables sequentially and each population automatically evaluates all the table columns
        for i, tab in enumerate(self.tables):
            tab.populate()

        # TODO: Build topology as a graph of mixed table population and column evaluation operations according to their depednecies
        # TODO: Execute operations in the topology by either populating a table or evaluating a column

        log.info("Finish executing workflow '{0}'.".format(self.id))


if __name__ == "__main__":
    pass
