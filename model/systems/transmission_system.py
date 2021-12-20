from systems.basic_system import EnergySystem
import pypsa


class TransmissionSystem(EnergySystem):

    def __init__(self, T, *args, **kwargs):
        super().__init__(T)
        self.transmission_model = pypsa.Network()
        self.edges = kwargs['edges']
        self.nodes = kwargs['nodes']

        for key, node in self.nodes.items():
            self.transmission_model.add('Bus', name=key, **node)

        for key, edge in self.edges.items():
            self.transmission_model.add('Line', name=key, **edge)

        self.generation = None
        self.demand = None

    def set_parameter(self, date, *args, **kwargs):
        pass

    def optimize(self):
        result_set = []

        for t in self.t:
            self.transmission_model.pf(distribute_slack=True)

