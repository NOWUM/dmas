import {Component, Input, OnInit} from '@angular/core';
import {ConfigService} from "../config.service";

@Component({
  selector: 'app-config-agents',
  templateUrl: './config-agents.component.html',
  styleUrls: ['./config-agents.component.css']
})
export class ConfigAgentsComponent implements OnInit {

  @Input() type: string;
  agentTypes: string[] = ['PWP', 'RES', 'DEM', 'STR', 'MRK', 'NET'];
  config: Map<string, string>;
  number: number;

  constructor(public service: ConfigService) {
    // Wird einmalig beim Erstellen eines Agents aufgerufen - immer gleich
    this.type = "agentX";
    this.config = new Map<string, string>();
    this.number = 0;
  }

  // Hier werden Agenten-spezifische Befehle einmalig aufrufen
  ngOnInit(): void {
    // Gibt den Typ des erstellten Agenten in der Console aus
    console.log('OnInit: ' + this.type);

    // Get agent specific config
    this.config = new Map<string, string>();
    this.get_config();

    // Get number of running agents per typ
    this.get_running_agents();
  }

  // update internal config map
  update_config(key: string, value: string): void {
    this.config.set(key, value);
    console.log(this.config);
  }

  get_config(){
    this.service.get_config(this.type)
      // clone the data object, using its known Config shape
      //.subscribe((data:Map<string, string>) => this.config = { ...data });//auslesen klappt, aber Error beim Ändern der Inputs: ERROR TypeError: this.config.set is not a function -> at ConfigAgentsComponent.update_config (config-agents.component.ts:61)

      // alte Variante von Rieke (klappt):
      .subscribe((data: any) => {
      //.subscribe((data: Map<string, string>) => {//Angabe des Datentyps klappt hier nicht, WARUM?

        console.log('get_config(): Subscription received for ' + this.type + ':');
        console.log(data);
        for (var value in data) {
          this.config.set(value, data[value]);
        }
      });
  }

  // Get number of running agents per typ
  get_running_agents(){
    console.log('get_running_agents(): subscribing for ' + this.type);
    this.service.get_running_agents(this.type).subscribe((data:number) => {
      this.number = data;
      console.log('get_running_agents(): Subscription received for ' + this.type + ': ' + this.number);
    }); //https://angular.io/guide/http
  }

}
