'use strict';

const path = require('node:path');
const Sim = require(path.resolve('out/sources/pokemon-showdown/dist/sim'));
const {Battle, Dex} = Sim;

const format = Dex.formats.get('[Gen 9 Champions] BSS Reg M-B', true);
const filler = n => ({species: ['Pikachu','Eevee','Bulbasaur','Squirtle','Charmander'][n % 5], level: 50, moves: ['Tackle']});
const attacker = {
  species: 'Garchomp', level: 50, ability: 'Rough Skin', item: 'Lum Berry', nature: 'Adamant',
  evs: {hp: 2, atk: 32, def: 0, spa: 0, spd: 0, spe: 32}, moves: ['Stone Edge'],
};
const defender = {
  species: 'Charizard-Mega-Y', level: 50, ability: 'Drought', item: 'Charizardite Y', nature: 'Modest',
  evs: {hp: 32, atk: 0, def: 32, spa: 2, spd: 0, spe: 0}, moves: ['Tackle'],
};
const p1 = [attacker, ...Array.from({length: 5}, (_, i) => filler(i))];
const p2 = [defender, ...Array.from({length: 5}, (_, i) => filler(i))];
const battle = new Battle({debug: true, format, seed: [1,2,3,4], p1: {team: p1}, p2: {team: p2}});
console.log('request-before', battle.requestState, battle.p1.active.length, battle.p2.active.length);
if (battle.requestState === 'teampreview') battle.makeChoices('team 123', 'team 123');
console.log('request-after', battle.requestState, battle.p1.active.length, battle.p2.active.length);
const source = battle.p1.active[0];
const target = battle.p2.active[0];
console.log('species', source.species.name, target.species.name);
console.log('stats', source.storedStats, target.storedStats, 'hp', source.maxhp, target.maxhp, 'weather', battle.field.weather);
battle.randomChance = () => false;
const move = battle.dex.getActiveMove('Stone Edge');
move.willCrit = false;
battle.randomizer = baseDamage => baseDamage;
const max = battle.actions.getDamage(source, target, move, true);
const move2 = battle.dex.getActiveMove('Stone Edge');
move2.willCrit = false;
battle.randomizer = baseDamage => Math.floor(baseDamage * 85 / 100);
const min = battle.actions.getDamage(source, target, move2, true);
console.log(JSON.stringify({min,max,targetHP:target.maxhp}));
battle.destroy();
