__author__="Alexandr Savinov"

import json

from lambdo.utils import *
from lambdo.resolve import *
from lambdo.transform import *

from lambdo.Workflow import *
from lambdo.Table import *
from lambdo.Column import *
from lambdo.Topology import *

import logging
log = logging.getLogger('lambdo.workflow')


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
        self.tables = self._create_tables_from_descriptions()

    def _create_tables_from_descriptions(self):
        """Create a list of Table objects by using their definitions in the workflow json."""
        tables_json = self.workflow_json.get("tables", [])
        tables = [Table(self,x) for x in tables_json]
        return tables

    def create_table(self, definition):
        """
        Create a new table object from its description provided in the argument.
        The description is appended to the definition of the workflow (if it exists).
        """

        # Instantiate a table object and append it to the list of all tables
        table = Table(self, definition)
        self.tables.append(table)

        # TODO: Append only if such a table (with the same id) does not exist yet
        if self.workflow_json:
            if self.workflow_json.get("tables") is None:
                self.workflow_json["tables"] = []
            self.workflow_json.get("tables").append(definition)

        return table

    def get_table(self, table_name):
        """Find a table object with the specified name"""
        if not table_name: return None
        return next((x for x in self.tables if x.id == table_name), None)

    def get_table_number(self, table_name):
        """Find table number within this workflow"""
        return next((i for i, x in enumerate(self.tables) if x.id == table_name), -1)

    def get_tables(self, table_names):
        """Find tables with the specified names"""
        if not table_names: return []
        tables = filter(lambda x: x.id in table_names, self.tables)
        return list(tables)

    #
    # Data operations
    #

    def execute(self):
        """
        Execute the whole workflow.
        This means executing all tables according to their dependencies.
        """
        log.info("Start executing workflow '{0}'.".format(self.id))

        topology = Topology(self)
        topology.translate()
        for layer in topology.layers:
            # Execute operations in one layer
            for elem in layer:
                if isinstance(elem, Table):
                    elem.populate()
                elif isinstance(elem, Column):
                    elem.evaluate()
                elif isinstance(elem, (tuple, list)):
                    if isinstance(elem[0], Table) and elem[1] == 'filter':
                        elem[0].execute_filter()
                    else:
                        pass  # Warning: wrong object type in an operation
                else:
                    pass  # Warning: wrong object type in an operation

        log.info("Finish executing workflow '{0}'.".format(self.id))


if __name__ == "__main__":
    pass
