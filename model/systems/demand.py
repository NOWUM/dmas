# model modules
from systems.basic_system import EnergySystem
from demandlib.electric_profile import StandardLoadProfile


class HouseholdModel(EnergySystem):

    def __init__(self, T, demandP, *args, **kwargs):
        super().__init__(T)
        self.profile_generator = StandardLoadProfile(demandP, type='household')


    def optimize(self):
        self.power = self.profile_generator.run_model(self.date)
        self.demand['power'] = self.power
        return self.power


class BusinessModel(EnergySystem):

    def __init__(self, T, demandP, *args, **kwargs):
        super().__init__(T)
        self.profile_generator = StandardLoadProfile(demandP, type='business')

    def optimize(self):
        self.power = self.profile_generator.run_model(self.date)
        self.demand['power'] = self.power
        return self.power


class IndustryModel(EnergySystem):

    def __init__(self, T, demandP, *args, **kwargs):
        super().__init__(T)
        self.profile_generator = StandardLoadProfile(demandP, type='industry')

    def optimize(self):
        self.power = self.profile_generator.run_model(self.date)
        self.demand['power'] = self.power
        return self.power

