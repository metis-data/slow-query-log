import express from 'express';
import { Client } from 'pg';
import { exampleQueries } from './exampleQueries';
import { MetisSqlCollector } from '@metis-data/slow-query-log';

let client: Client;
let metis: MetisSqlCollector;
async function setClient() {
  metis = new MetisSqlCollector();
  await metis.setup();

  const res = await metis.run();

  console.log(res);
}
setClient();

const app = express();

app.use(express.json());

app.get('/person', async (req, res) => {
  const c = await client.query(exampleQueries.person);
  return res.json(c.rows);
});

app.get('/person/:id', async (req, res) => {
  const c = await client.query(exampleQueries.personById, [req.params.id]);
  return res.json(c.rows);
});

app.get('/person-insert', async (req, res) => {
  const c = await client.query(exampleQueries.personInsert);
  return res.json(c);
});

app.get('/person-delete', async (req, res) => {
  const c = await client.query(exampleQueries.personDelete);
  return res.json(c);
});

app.listen(3000, () => console.log(`🚀 Server ready at: http://localhost:3000`));
