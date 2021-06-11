/* agents-info.component.ts
|   Info: Übersicht und Verwaltung aller laufenden Agenten
|   Typ: TS Logic
|   Inhalt: Alle Ageneten sortiert nach Typ auf einer Seite anzeigen mit der Option, diese einzeln stoppen zu können
|   TODO: Filterlogik implementieren (angezeigte Agenten nach Typ filtern)
*/

import { Component, OnInit } from '@angular/core';
import {ConfigService} from "../config.service";

@Component({
  selector: 'app-agents-info',
  templateUrl: './agents-info.component.html',
  styleUrls: ['./agents-info.component.css']
})
export class AgentsInfoComponent implements OnInit {
  agentTypes: string[] = ['PWP', 'RES', 'DEM', 'STR', 'MRK', 'NET'];
  agents: Map<string, string>;

  showAgents: Map<string,any>;

  constructor(public service: ConfigService) {
    this.agents = new Map<string, string>();
    this.showAgents = new Map<string, boolean>();
  }

  ngOnInit(): void {
    for (var value in this.agentTypes) {
        this.showAgents.set(this.agentTypes[value], true);
      }
    console.log('showAgents:');
    console.log(this.showAgents);
  }

  // An-/Ausschaltlogik der Checkboxes für jeden Agent Typ
  togglePwp() { this.showAgents.set('PWP', !this.showAgents.get('PWP')); }
  toggleRes() { this.showAgents.set('RES', !this.showAgents.get('RES')); }
  toggleDem() { this.showAgents.set('DEM', !this.showAgents.get('DEM')); }
  toggleStr() { this.showAgents.set('STR', !this.showAgents.get('STR')); }
  toggleMrk() { this.showAgents.set('MRK', !this.showAgents.get('MRK')); }
  toggleNet() { this.showAgents.set('NET', !this.showAgents.get('NET')); }

  // Logik, ob Typ angezeigt werden soll
  showType(inType:string = 'default'){
    if(this.showAgents.get(inType)) return true;
    return false;
  }

  // Laufende Agenten abrufen
  get_agents(inType:string = 'NET'){
    let test = 0;
    this.service.get_info(inType).subscribe((data: any) => {
      console.log(data);
      for (var value in data) {
        this.agents.set(value, data[value]);
      }
      return test;
    });
  }

}
