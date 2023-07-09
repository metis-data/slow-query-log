"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
const express_1 = require("express");
const pg_1 = require("pg");
const child_process_1 = require("child_process");
const exampleQueries_1 = require("./exampleQueries");
let client;
function setClient() {
    return __awaiter(this, void 0, void 0, function* () {
        const connectionString = process.env.PG_CONNECTION_STRING;
        client = new pg_1.Client({ connectionString });
        yield client.connect();
        (0, child_process_1.execSync)(`psql -U postgres -d platform-v2 -a -f ./dump.sql`);
    });
}
setClient();
const app = (0, express_1.default)();
app.use(express_1.default.json());
app.get('/person', (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    const c = yield client.query(exampleQueries_1.exampleQueries.person);
    return res.json(c.rows);
}));
app.get('/person/:id', (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    const c = yield client.query(exampleQueries_1.exampleQueries.personById, [req.params.id]);
    return res.json(c.rows);
}));
app.get('/person-insert', (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    const c = yield client.query(exampleQueries_1.exampleQueries.personInsert);
    return res.json(c);
}));
app.get('/person-delete', (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    const c = yield client.query(exampleQueries_1.exampleQueries.personDelete);
    return res.json(c);
}));
app.listen(3000, () => console.log(`🚀 Server ready at: http://localhost:3000`));
//# sourceMappingURL=index.js.map