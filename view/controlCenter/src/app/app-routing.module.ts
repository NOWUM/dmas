/* app-routing.module.ts
|   Info: Angular Routing Module
|   Typ: TS Logic
|   Inhalt: Festlegung der Routes (welcher Anhang zu welcher Component weiterleiten soll)
*/

import {RouterModule, Routes} from "@angular/router";
import {NgModule} from "@angular/core";
import {GridComponent} from "./grid/grid.component";
import {ConfigComponent} from "./config/config.component";
import {ConfigAgentsComponent} from "./config-agents/config-agents.component";
import {AgentsInfoComponent} from "./agents-info/agents-info.component";

const routes: Routes = [
  {path: '', redirectTo: '/config', pathMatch: 'full'},
  {path: 'config', component: ConfigComponent},
  {path: 'agents-info', component: AgentsInfoComponent},
  {path: 'grid', component: GridComponent}
];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule {}
export const routingComponents = [ConfigComponent,AgentsInfoComponent,GridComponent]
