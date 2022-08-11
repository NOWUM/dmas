# model modules
from systems.basic_system import EnergySystem
from demandlib.electric_profile import StandardLoadProfile
import pandas as pd

class HouseholdModel(EnergySystem):

    def __init__(self, T: int, demandP: float, *args, **kwargs):
        super().__init__(T)
        self.profile_generator = StandardLoadProfile(demandP, type='household')

    def optimize(self, date=None, weather=None, prices=None, steps=None):
        """
        run profile generator for current day
        :return: timer series in [kW]
        """
        self.power = self.profile_generator.run_model(pd.to_datetime(self.date))
        self.demand['power'] = self.power
        return self.power


class BusinessModel(EnergySystem):

    def __init__(self, T: int, demandP: float, *args, **kwargs):
        super().__init__(T)
        self.profile_generator = StandardLoadProfile(demandP, type='business')

    def optimize(self, date=None, weather=None, prices=None, steps=None):
        """
        run profile generator for current day
        :return: timer series in [kW]
        """
        self.power = self.profile_generator.run_model(self.date)
        self.demand['power'] = self.power
        return self.power


class IndustryModel(EnergySystem):

    def __init__(self, T: int, demandP: float, *args, **kwargs):
        super().__init__(T)
        self.profile_generator = StandardLoadProfile(demandP, type='industry')

    def optimize(self, date=None, weather=None, prices=None, steps=None):
        """
        run profile generator for current day
        :return: timer series in [kW]
        """
        self.power = self.profile_generator.run_model(self.date)
        self.demand['power'] = self.power
        return self.power

class AgricultureModel(EnergySystem):

    def __init__(self, T: int, demandP: float, *args, **kwargs):
        super().__init__(T)
        self.profile_generator = StandardLoadProfile(demandP, type='agriculture')


    def optimize(self):
        """
        run profile generator for current day
        :return: timer series in [kW]
        """
        self.power = self.profile_generator.run_model(self.date)
        self.demand['power'] = self.power
        return self.power
