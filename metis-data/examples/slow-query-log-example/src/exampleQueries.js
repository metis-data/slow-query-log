"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.exampleQueries = void 0;
exports.exampleQueries = {
    person: 'SELECT * FROM person',
    personById: 'SELECT * FROM person WHERE id = $1',
    personInsert: 'INSERT INTO "person" ("firstName", "lastName", "age") VALUES (\'john\', \'doe\', 42) ON CONFLICT DO NOTHING',
    personDelete: 'DELETE FROM "person" WHERE "firstName" = \'john\' AND "lastName" = \'doe\'',
};
//# sourceMappingURL=exampleQueries.js.map