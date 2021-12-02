# model modules
from systems.basic_system import EnergySystem
from demandlib.standard_load_profile import StandardLoadProfile


class HouseholdModel(EnergySystem):

    def __init__(self, T, demandP, *args, **kwargs):
        super().__init__(T)
        self.profile_generator = StandardLoadProfile(demandP, type='household')


    def optimize(self):
        self.profile_generator.run_model(self.date)
        return self.power


class BusinessModel(EnergySystem):

    def __init__(self, T, demandP, *args, **kwargs):
        super().__init__(T)
        self.profile_generator = StandardLoadProfile(demandP, type='business')

    def optimize(self):
        self.profile_generator.run_model(self.date)
        return self.power


class IndustryModel(EnergySystem):

    def __init__(self, T, demandP, *args, **kwargs):
        super().__init__(T)
        self.profile_generator = StandardLoadProfile(demandP, type='industry')

    def optimize(self):
        self.profile_generator.run_model(self.date)
        return self.power
