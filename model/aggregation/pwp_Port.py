import numpy as np
from components.pwp_Plant import powerPlant_gurobi as power_plant
from components.pwp_Storage import storage_gurobi as storage
from aggregation.basic_Port import PortfolioModel
from gurobipy import *


class PwpPort(PortfolioModel):

    def __int__(self, T=24, dt=1, gurobi=True, date='2020-01-01'):
        super().__init__(T, dt, gurobi, date, typ)

    def add_energy_system(self, name, energysystem):
        data = energysystem[name]

        # -- PWP
        if data['typ'] == 'powerPlant':              # konv. power plant
            data.update(dict(model=power_plant(t=self.t, T=self.T, dt=self.dt, model=self.m)))
        elif data['typ'] == 'storage':           # storage plant
            data.update(dict(model=storage(t=self.t, T=self.T, dt=self.dt, model=self.m)))

        self.energySystems.update(energysystem)

    def build_model(self, response=[], max_=False, min_=False):
        # ----- remove all constrains and vars -----
        self.m.remove(self.m.getVars())
        self.m.remove(self.m.getConstrs())
        # ----- build up each system -----
        for key, data in self.energySystems.items():
            data['model'].build(key, data, self.prices)

        # ----- build up aggregation constrains and vars -----

        # Summe Leistung
        power = self.m.addVars(self.t, vtype=GRB.CONTINUOUS, name='P', lb=-GRB.INFINITY, ub=GRB.INFINITY)
        self.m.addConstrs(power[i] == quicksum(p for p in [x for x in self.m.getVars() if 'P_' in x.VarName]
                                               if '[%i]' % i in p.VarName) for i in self.t)

        # Summe Brennstoffkosten
        fuel = self.m.addVars(self.t, vtype=GRB.CONTINUOUS, name='F', lb=-GRB.INFINITY, ub=GRB.INFINITY)
        self.m.addConstrs(fuel[i] == quicksum(f for f in [x for x in self.m.getVars() if 'F_' in x.VarName]
                                              if '[%i]' % i in f.VarName) for i in self.t)
        # Summe Emissionkosten
        emission = self.m.addVars(self.t, vtype=GRB.CONTINUOUS, name='E', lb=-GRB.INFINITY, ub=GRB.INFINITY)
        self.m.addConstrs(emission[i] == quicksum(e for e in [x for x in self.m.getVars() if 'E_' in x.VarName]
                                                  if '[%i]' % i in e.VarName) for i in self.t)
        # Summe Erlöse
        profit = self.m.addVar(vtype=GRB.CONTINUOUS, name='Profit', lb=-GRB.INFINITY, ub=GRB.INFINITY)
        self.m.addConstr(profit == quicksum(power[i] * self.prices['power'][i] for i in self.t))

        if len(response) == 0:
            if max_:
                self.m.setObjective(quicksum(power[i] for i in self.t), GRB.MAXIMIZE)
            elif min_:
                cashflow = self.m.addVars(self.t, vtype=GRB.CONTINUOUS, name='cash', lb=-GRB.INFINITY, ub=GRB.INFINITY)
                self.m.addConstrs((cashflow[i] == (power[i] * self.prices['power'][i]) - emission[i] - fuel[i])
                                  for i in self.t)
                minus = self.m.addVars(self.t, vtype=GRB.CONTINUOUS, name='minus', lb=0, ub=GRB.INFINITY)
                plus = self.m.addVars(self.t, vtype=GRB.CONTINUOUS, name='plus', lb=0, ub=GRB.INFINITY)
                self.m.addConstrs((cashflow[i] == -minus[i] + plus[i]) for i in self.t)
                self.m.setObjective(quicksum(minus[i] + plus[i] for i in self.t), GRB.MINIMIZE)
            else:
                # objective function (max cashflow)
                self.m.setObjective(profit - quicksum(fuel[i] + emission[i] for i in self.t), GRB.MAXIMIZE)
        else:
            powerReBAP = self.m.addVars(self.t, vtype=GRB.CONTINUOUS, name='ReBA', lb=0, ub=GRB.INFINITY)
            minus = self.m.addVars(self.t, vtype=GRB.CONTINUOUS, name='minus', lb=0, ub=GRB.INFINITY)
            plus = self.m.addVars(self.t, vtype=GRB.CONTINUOUS, name='plus', lb=0, ub=GRB.INFINITY)
            self.m.addConstrs((response[i] - power[i] == -minus[i] + plus[i]) for i in self.t)
            self.m.addConstrs(minus[i] + plus[i] == powerReBAP[i] for i in self.t)
            # self.m.setObjective(quicksum(self.fuel[i] + self.emission[i] +
            #                              powerReBAP[i] * (np.abs(self.prices['power'][i]) + 35) for i in self.t),
            #                     GRB.MINIMIZE)
            self.m.setObjective(quicksum(powerReBAP[i] for i in self.t), GRB.MINIMIZE)
        # ----- update model -----
        self.m.update()

    def optimize(self):

        power = np.zeros_like(self.t, np.float)               # total portfolio generation
        emission = np.zeros_like(self.t, np.float)            # total portfolio emission costs
        fuel = np.zeros_like(self.t, np.float)                # total portfolio fuel costs
        self.volume = np.zeros_like(self.t, np.float)
        try:
            self.m.optimize()
            #  total power [MW] for each hour
            power = np.asarray([self.m.getVarByName('P[%i]' % i).x for i in self.t], np.float).reshape((-1,))
            power = np.round(power, 2)
            # total emissions costs [€] for each hour
            emission = np.asarray([self.m.getVarByName('E[%i]' % i).x for i in self.t], np.float).reshape((-1,))
            emission = np.round(emission, 2)
            # total fuel costs [€] for each hour
            fuel = np.asarray([self.m.getVarByName('F[%i]' % i).x for i in self.t], np.float).reshape((-1,))
            fuel = np.round(fuel, 2)
            # initialize dict for fuel sum calculation
            generation = dict(powerLignite=np.zeros_like(self.t, np.float),          # total generation lignite   [MW]
                              powerCoal=np.zeros_like(self.t, np.float),             # total generation caol      [MW]
                              powerGas=np.zeros_like(self.t, np.float),              # total generation gas       [MW]
                              powerNuc=np.zeros_like(self.t, np.float),              # total generation nuc       [MW]
                              powerWater=np.zeros_like(self.t, np.float))            # total generation storages  [MW]

            for key, value in self.energySystems.items():
                # set output power for each energy system (power plant)
                value['model'].power = np.asarray([self.m.getVarByName('P' + '_%s[%i]' % (key, i)).x
                                                   for i in self.t], np.float).reshape((-1,))
                value['model'].emission = np.asarray([self.m.getVarByName('E' + '_%s[%i]' % (key, i)).x
                                                      for i in self.t], np.float).reshape((-1,))
                value['model'].fuel = np.asarray([self.m.getVarByName('F' + '_%s[%i]' % (key, i)).x
                                                  for i in self.t], np.float).reshape((-1,))
                # add generation to corresponding fuel typ
                generation['power%s' % value['fuel'].capitalize()] += value['model'].power
                value['model'].volume = np.zeros_like(power)
                # if power plant typ = storage set volume [MWh]
                if value['typ'] == 'storage':
                    volume = np.asarray([self.m.getVarByName('V' + '_%s[%i]' % (key, i)).x
                                         for i in self.t], np.float).reshape((-1,))
                    value['model'].volume = volume
                    self.volume += volume
            # save result in generation dictionary
            for key, value in generation.items():
                self.generation[key] = value
            self.generation['powerTotal'] = power

        except Exception as e:
            for key, value in self.energySystems.items():
                value['model'].power = np.zeros_like(power)
                value['model'].volume = np.zeros_like(power)
            print(e)

        self.power = power
        self.emission = emission
        self.fuel = fuel

        return self.power, self.emission, self.fuel

    def fix_planing(self):
        power = np.zeros_like(self.t, np.float)               # total portfolio generation
        emission = np.zeros_like(self.t, np.float)            # total portfolio emission costs
        fuel = np.zeros_like(self.t, np.float)                # total portfolio fuel costs
        self.volume = np.zeros_like(self.t, np.float)
        try:
            self.m.optimize()
            #  total power [MW] for each hour
            power = np.asarray([self.m.getVarByName('P[%i]' % i).x for i in self.t], np.float).reshape((-1,))
            power = np.round(power, 2)
            # total emissions costs [€] for each hour
            emission = np.asarray([self.m.getVarByName('E[%i]' % i).x for i in self.t], np.float).reshape((-1,))
            emission = np.round(emission, 2)
            # total fuel costs [€] for each hour
            fuel = np.asarray([self.m.getVarByName('F[%i]' % i).x for i in self.t], np.float).reshape((-1,))
            fuel = np.round(fuel, 2)
            # initialize dict for fuel sum calculation
            generation = dict(powerLignite=np.zeros_like(self.t, np.float),          # total generation lignite   [MW]
                              powerCoal=np.zeros_like(self.t, np.float),             # total generation caol      [MW]
                              powerGas=np.zeros_like(self.t, np.float),              # total generation gas       [MW]
                              powerNuc=np.zeros_like(self.t, np.float),              # total generation nuc       [MW]
                              powerWater=np.zeros_like(self.t, np.float))            # total generation storages  [MW]

            for key, value in self.energySystems.items():
                # set output power for each energy system (power plant)
                value['model'].power = np.asarray([self.m.getVarByName('P' + '_%s[%i]' % (key, i)).x
                                                   for i in self.t], np.float).reshape((-1,))
                value['model'].emission = np.asarray([self.m.getVarByName('E' + '_%s[%i]' % (key, i)).x
                                                      for i in self.t], np.float).reshape((-1,))
                value['model'].fuel = np.asarray([self.m.getVarByName('F' + '_%s[%i]' % (key, i)).x
                                                  for i in self.t], np.float).reshape((-1,))
                # add generation to corresponding fuel typ
                generation['power%s' % value['fuel'].capitalize()] += value['model'].power
                value['model'].volume = np.zeros_like(power)
                # if power plant typ = storage set volume [MWh]
                if value['typ'] == 'storage':
                    volume = np.asarray([self.m.getVarByName('V' + '_%s[%i]' % (key, i)).x
                                         for i in self.t], np.float).reshape((-1,))
                    value['model'].volume = volume
                    self.volume += volume

            # save result in generation dictionary
            for key, value in generation.items():
                self.generation[key] = value
            self.generation['powerTotal'] = power

            for key, value in self.energySystems.items():
                if value['typ'] == 'powerPlant':
                    value['P0'] = [np.round(self.m.getVarByName('P_%s[%i]' % (key, i)).x, 2) for i in self.t[:24]][-1]
                    z = np.asanyarray([self.m.getVarByName('z_%s[%i]' % (key, i)).x for i in self.t[:24]], np.float)

                    if z[-1] > 0:
                        index = -1 * value['runTime']
                        value['on'] = np.count_nonzero(z[index:])
                        value['off'] = 0
                    else:
                        index = -1 * value['stopTime']
                        value['off'] = np.count_nonzero(1 - z[index:])
                        value['on'] = 0

                    value['model'].power = [self.m.getVarByName('P' + '_%s[%i]' % (key, i)).x for i in self.t]
                    value['model'].volume = np.zeros_like(power)

                if value['typ'] == 'storage':
                    value['P+0'] = [self.m.getVarByName('P+_%s[%i]' % (key, i)).x for i in self.t[:24]][-1]
                    value['P-0'] = [self.m.getVarByName('P-_%s[%i]' % (key, i)).x for i in self.t[:24]][-1]
                    value['V0'] = [self.m.getVarByName('V_%s[%i]' % (key, i)).x for i in self.t[:24]][-1]
                    value['model'].volume = [self.m.getVarByName('V' + '_%s[%i]' % (key, i)).x for i in self.t]

                    value['model'].power = [self.m.getVarByName('P' + '_%s[%i]' % (key, i)).x for i in self.t]
                    value['model'].volume = [self.m.getVarByName('V' + '_%s[%i]' % (key, i)).x for i in self.t]

        except Exception as e:
            for key, value in self.energySystems.items():
                value['model'].power = np.zeros_like(power)
                value['model'].volume = np.zeros_like(power)
            print(e)

        self.power = power
        self.emission = emission
        self.fuel = fuel

        return self.power, self.emission, self.fuel


if __name__ == "__main__":
    pass
