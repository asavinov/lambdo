__author__="Alexandr Savinov"

import json
import copy

from lambdo.utils import *
from lambdo.resolve import *
from lambdo.transform import *

from lambdo.Workflow import *
from lambdo.Table import *
from lambdo.Column import *

import logging
log = logging.getLogger('lambdo.topology')


class Topology:
    """
    The class represents a topology.
    Topology is a graph of operations on data where one operation is either a table population or column evaluation.
    """

    def __init__(self, workflow):

        # We will probably modify it during translation create a copy in order not to change the original definitions
        self.workflow = workflow  # copy.deepcopy(workflow)

        # Here we store the graph of translated and executable operations
        self.layers = []

    def translate(self):
        """
        Build a graph of operations by analyzing table and column definitions.
        The graph consists of table, column and possibly other operations.
        New operations can be added if necessary.
        """

        #
        # Loop through all user provided definitions and add all the necessary (maybe additional) operations to the list
        #
        all_operations = []
        for top in self.workflow.tables:

            # Add table population operation
            all_operations.append(top)

            # Add all column definitions
            for cop in top.columns:
                # Get all columns this column (functional) operation will consume in standard format 'Table::Link::Column'
                #in_columns = cop.get_input_columns()
                # TODO: If there is a complex column in this list, and it does not exist yet in the list, then add its definition

                all_operations.append(cop)


            # Add filter operation (Table, 'filter') if it has been defined in the table
            if top.has_filters():
                all_operations.append((top, 'filter'))

        #
        # Build graph of operations by analyzing dependencies
        #

        # Empty collection of already processed elements (they can be simultaneously removed from all)
        done = []
        # Topology to be built is a list of layers in the order of execution of their operations. First layer does not have dependencies.
        layers = []
        while True:
            layer = []

            # We will build this new layer from the available (not added to previous layers) operations
            for elem in all_operations:

                if elem in done:
                    continue

                #
                # Find dependencies of this operation
                #
                if isinstance(elem, (Table, Column)):
                    deps = elem.get_dependencies()  # Get all element definitions this element depends upon
                elif isinstance(elem, (tuple, list)) and isinstance(elem[0], Table):
                    deps = [elem[0]]  # Filter depends on its table
                    deps.extend(elem[0].columns)  # Filter is applied only after all columns have been evaluated
                else:
                    pass  # Error: unknown operation

                #
                # If all dependencies have been executed then add this element to the current layer
                #
                if set(deps) <= set(done):
                    layer.append(elem)

            if len(layer) == 0:
                break

            layers.append(layer)
            done.extend(layer)

        self.layers = layers

        return layers


if __name__ == '__main__':
    pass
