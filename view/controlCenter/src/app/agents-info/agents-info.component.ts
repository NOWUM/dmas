/* agents-info.component.ts
|   Info: Übersicht und Verwaltung aller laufenden Agenten
|   Typ: TS Logic
|   Inhalt: Alle Ageneten sortiert nach Typ auf einer Seite anzeigen mit der Option, diese einzeln stoppen zu können
|   Funktionen: Service Config anzeigen, konfigurieren und updaten
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

  showPwp = true;
  showRes = true;
  DEM = true;
  showStr = true;
  showMrk = true;
  showNet = true;

  constructor(public service: ConfigService) {
    this.agents = new Map<string, string>();
  }

  ngOnInit(): void {
  }

  // An-/Ausschaltlogik der Checkboxes für jeden Agent Typ
  togglePwp() { this.showPwp = !this.showPwp; }
  toggleRes() { this.showRes = !this.showRes; }
  toggleDem() { this.DEM = !this.DEM; }
  toggleStr() { this.showStr = !this.showStr; }
  toggleMrk() { this.showMrk = !this.showMrk; }
  toggleNet() { this.showNet = !this.showNet; }

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
