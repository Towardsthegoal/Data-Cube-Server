const { Pool } = require("pg");

const compareDataTypes = require("../utils/table");
const { query } = require("express");

let pool;

const dataType = {
  character: "Text",
  "character varying": "Text",
  text: "Text",
  bigint: "Integer",
  smallint: "Integer",
  integer: "Integer",
  numeric: "Decimal",
  "double precision": "Decimal",
  bit: "Boolean",
  "bit varying": "Boolean",
  boolean: "Boolean",
  date: "Date",
  "timestamp without time zone": "TimeStamp",
  "timestamp with time zone": "TimeStamp",
};

const setDBInfo = async (req, res) => {
  // const { host, username, password, port, db } = req.body;
  // const connectionString = `postgres://${username}:${password}@${host}:${port}/${db}`;
  const connectionString = `postgres://postgres:password@localhost:5432/test`;
  pool = new Pool({ connectionString, ssl: false });

  pool.connect((err, client, release) => {
    if (err) {
      console.log("Error establishing pool connection:", err);
      //   res.status(400).json({ success: false });
    } else {
      console.log("Pool connection established successfully");
      //   res.status(200).json({ success: true });
      release();
    }
  });
};

const getTables = async (req, res) => {
  try {
    await setDBInfo();
    const client = await pool.connect();
    const query = `SELECT table_name
      FROM information_schema.tables
      WHERE table_schema = 'public'
      AND table_type = 'BASE TABLE'`;
    const allTables = await client.query(query);
    let realTables = [];
    allTables.rows.forEach((item) => {
      let tableName = item["table_name"];
      if (tableName.indexOf("_map") < 0) {
        realTables.push(tableName);
      }
    });
    res.json({ data: realTables });
  } catch (err) {
    console.log("getTables error", err);
  }
};

const getTableData = async (req, res) => {
  try {
    await setDBInfo();
    const client = await pool.connect();
    const { table } = req.body;
    console.log(req)
    const query = `SELECT *
      FROM ${table}
      Order By id`;
    const tableData = await client.query(query);
    // console.log(tableData.rows)
    res.json({ data: tableData.rows });
  } catch (err) {
    console.log("getTables error", err);
  }
};

/*
 *Router: /mapData
 *Function: Make map tables from dimension tables
 */
const mapData = async (req, res) => {
  try {
    await setDBInfo();
    const client = await pool.connect();
    const dimTables = req.body.dimTables.split(",");
    //Create Map Table with each dimension table
    dimTables.forEach(async (dim) => {
      //Read Table Rows
      dim = dim.trim().toLowerCase();
      const tableData = await client.query(`Select * From "${dim}"`);
      const nodes = tableData.rows.map((row) => {
        return { id: row.id, parent_id: row.parent_id };
      });
      //Generate Mapping Table with Parents
      let mapping = generateMapping(nodes);
      //Drop Existed Table
      const existTable = await client.query(
        `SELECT table_name FROM information_schema.tables WHERE table_name = '${dim}_map'`
      );
      if (existTable.rowCount > 0) {
        await client.query(`Drop Table ${dim}_map`);
      }
      //Create New Map Table
      const createTableQuery = `Create Table ${dim}_map (
            "${dim}_id" Integer NOT NULL,
            "parent_${dim}_id" Integer NOT NULL
        );`;
      await client.query(createTableQuery);
      //Insert Mapping data
      mapping.forEach((map) => {
        map.parents.forEach(async (parent) => {
          await client.query(
            `Insert Into ${dim}_map (${dim}_id, parent_${dim}_id) Values (${map.id}, ${parent})`
          );
        });
      });
    });
    res.json({ msg: "Mappings generated" });
  } catch (error) {
    console.log(error);
  }
};

/*
 *Function: Mapping cells with their parents
 */
const generateMapping = (nodes) => {
  let mapping = [];
  nodes.forEach((node) => {
    let parents = [];
    let parentID = node.id;
    while (parentID != null) {
      parents.push(parentID);
      parentNode = nodes.filter((val) => {
        return val.id === parentID;
      });
      parentID = parentNode[0].parent_id;
    }
    mapping.push({ id: node.id, parents: parents });
  });
  return mapping;
};

/*
 *Router: /queryCell
 *Function: Return cell from query
 */
const queryCell = async (req, res) => {
  try {
    await setDBInfo();
    const client = await pool.connect();
    const dim = JSON.parse(req.body.toString());
    let factTable = "sales";
    let factCol = "sales";
    let aggregate = "sum";
    let query = `Select ${aggregate}(${factCol}) From ${factTable} `;
    query += `Where 1=1`;
    for (const key in dim)
      query += `\nAnd ${key}_id in (Select ${key}_id From ${key}_map, ${key} Where ${key}_map.parent_${key}_id = ${key}.id And (${key}.name='${dim[key]}' Or ${key}.alias='${dim[key]}'))`;

    console.log(query);
    const result = await client.query(query);
    res.json(result.rows[0]);
    client.end();
  } catch (error) {
    console.log(error);
  }
};

/*
 *Router: /pivot
 *Function: Return cell from multi-dimension query
 */
const pivot = async (req, res) => {
  try {
    await setDBInfo();
    const client = await pool.connect();
    let { rows, cols, filters } = JSON.parse(req.body.toString());

    const { success, error } = validateDimensions(rows, cols, filters);
    if (!success) {
      res.json({ error });
    }

    // rows = await expandRelation(rows[0][0]);
    // cols = await makeChildren(cols)

    let expandedRows = [];
    for (const row of rows) {
      let expandedRowItem = [];
      for (const rowItem of row) {
        let tmpExpandedRowItem = await expandRelation(rowItem);
        expandedRowItem.push(tmpExpandedRowItem);
      }
      expandedRows.push(expandedRowItem);
    }

    let expandedCols = [];
    for (const col of cols) {
      let expandedColItem = [];
      for (const colItem of col) {
        let tmpexpandedColItem = await expandRelation(colItem);
        expandedColItem.push(tmpexpandedColItem);
      }
      expandedCols.push(expandedColItem);
    }
    // console.log("expandedRows[0]--------", expandedRows[0])
    let matchedRows = [];
    for (const item of expandedRows) {
      matchedRows.push(generateCombination(item));
    }

    let matchedCols = [];
    for (const item of expandedCols) {
      matchedCols.push(generateCombination(item));
    }

    let returnData = {};

    let returnColumns = [];
    for (const col of matchedCols) {
      for (const colItem of col) {
        let tmpColumns = {};
        for (const item of colItem) {
          tmpColumns[`${item.dimension}`] = item.member;
        }
        returnColumns.push(tmpColumns);
      }
    }
    returnData["columns"] = returnColumns;

    let returnRows = [];
    for (const row of matchedRows) {
      for (const rowItem of row) {
        let tmpreturnRows = {};
        for (const item of rowItem) {
          tmpreturnRows[`${item.dimension}`] = item.member;
        }
        returnRows.push(tmpreturnRows);
      }
    }
    returnData["rows"] = returnRows;

    let data = [];

    for (const row of matchedRows) {
      for (const rowItem of row) {
        let tmpData = [];
        for (const col of matchedCols) {
          for (const colItem of col) {
            // console.log("rowItem, colItem", rowItem, colItem);
            let query = buildQuery(rowItem, colItem, filters);
            let result = await client.query(query);
            tmpData.push(result.rows[0]["result"]);
            // console.log("query", query)
            // console.log("query result ", result.rows)
          }
        }
        data.push(tmpData);
      }
    }
    returnData["data"] = data;

    console.log(returnData);
    res.json({ Return: returnData });
    // let factCol = "sales";
    // let factTable = "sales";

    // let rowData = {};
    // for (const row of rows) {
    //   let colData = {};
    //   for (const col of cols) {
    //     let tables = factTable;
    //     let selectItems =  `${col[0].aggregate}(${factCol}) as sum`;
    //     let where = "Where 1 = 1";
    //     let groupby = ""

    //     for (let i = 0; i < row.length; i++) {
    //       tables += `, ${row[i].dimension}`;
    //       selectItems += `, ${row[i].dimension}.name as ${row[i].dimension}_name`;
    //       where += ` And ${factTable}.${row[i].dimension}_id = ${row[i].dimension}.id`;
    //       if(i === 0)
    //         groupby += `Group By ${row[i].dimension}_name`
    //       else
    //         groupby += `, ${row[i].dimension}_name`
    //     }
    //     for (let i = 0; i < col.length; i++) {
    //       tables += `, ${col[i].dimension}`;
    //       selectItems += `, ${col[i].dimension}.name as ${col[i].dimension}_name`;
    //       where += ` And ${factTable}.${col[i].dimension}_id = ${col[i].dimension}.id`;
    //       groupby += `, ${col[i].dimension}_name`
    //     }

    //     for (const rowItem of row) {
    //       where += ` And ${factTable}.${rowItem.dimension}_id = ${rowItem.dimension}.id`;
    //     }
    //     for (const colItem of col) {
    //       where += ` And ${factTable}.${colItem.dimension}_id = ${colItem.dimension}.id`;
    //     }

    //     let query = `Select ${selectItems} \nFrom ${tables} \n${where}`;
    //     let aggregateQuery = `Select ${col[0].aggregate}(${factCol}) as sum \nFrom ${tables} \n${where}`

    //     for (const rowItem of row) {
    //       query += `\nAnd ${rowItem.dimension}.id in (${relationQuery(
    //         rowItem
    //       )})`;
    //       aggregateQuery += `\nAnd ${rowItem.dimension}.id in (${relationQuery(
    //         rowItem
    //       )})`;
    //     }
    //     for (const colItem of col) {
    //       query += `\nAnd ${colItem.dimension}.id in (${relationQuery(
    //         colItem
    //       )})`;
    //       aggregateQuery += `\nAnd ${colItem.dimension}.id in (${relationQuery(
    //         colItem
    //       )})`;
    //     }
    //     query += `\n${groupby}`

    //     let colDataKey = "";
    //     for (let i = 0; i < col.length; i++) {
    //       if (i === col.length - 1) {
    //         if (col[i].hasOwnProperty("relation"))
    //           colDataKey += `${col[i].dimension}_${col[i].member}_${col[i].relation}`;
    //         else colDataKey += `${col[i].dimension}_${col[i].member}`;
    //       } else {
    //         if (col[i].hasOwnProperty("relation"))
    //           colDataKey += `${col[i].dimension}_${col[i].member}_${col[i].relation}>`;
    //         else colDataKey += `${col[i].dimension}_${col[i].member}>`;
    //       }
    //     }
    //     // console.log("query", query)
    //     let result = await client.query(query);
    //     let resultRows = result.rows;

    //     let aggregateResult = await client.query(aggregateQuery)
    //     aggregateResult = aggregateResult.rows[0]['sum']

    //     let aggregateData = {}
    //     aggregateData["sum"] = aggregateResult;
    //     for(const rowItem of row){
    //       aggregateData[`${rowItem.dimension}_name`] = rowItem.member
    //     }
    //     for(const colItem of col){
    //       aggregateData[`${colItem.dimension}_name`] = colItem.member
    //     }

    //     colData[colDataKey] = resultRows;
    //     colData[colDataKey] = [...colData[colDataKey], aggregateData]
    //     // console.log(colData);
    //     // console.log("query result", result.rows)
    //   }
    //   let rowDataKey = "";
    //   for (let i = 0; i < row.length; i++) {
    //     if (i === row.length - 1) {
    //       if (row[i].hasOwnProperty("relation"))
    //         rowDataKey += `${row[i].dimension}_${row[i].member}_${row[i].relation}`;
    //       else rowDataKey += `${row[i].dimension}_${row[i].member}`;
    //     } else {
    //       if (row[i].hasOwnProperty("relation"))
    //         rowDataKey += `${row[i].dimension}_${row[i].member}_${row[i].relation}>`;
    //       else rowDataKey += `${row[i].dimension}_${row[i].member}>`;
    //     }
    //   }
    //   rowData[rowDataKey] = colData
    // }
    // // console.log("rowData-------------------", rowData)
    // res.json({data:rowData})
  } catch (error) {
    console.log("pivot error", error);
  }
};

/*
 *Function: Validate whether dimension is duplicated
 */
const validateDimensions = (rows, cols, filters) => {
  let rowDimensions = [];
  let colDimensions = [];

  if (rows) {
    //Store dimensions that rows contains
    for (const row of rows) {
      let lastDims = [];
      for (const rowItem of row) {
        //Check whether each row has same dimensions
        if (lastDims.indexOf(rowItem.dimension) > -1)
          return {
            sucess: false,
            error: `${rowItem.dimension} dimension in rows is duplicated!`,
          };
        else lastDims.push(rowItem.dimension);
      }
      rowDimensions.push(...lastDims);
    }

    //Clean duplicated dimensions of rows
    rowDimensions = rowDimensions.filter((value, index, self) => {
      return self.indexOf(value) === index;
    });
  }

  if (cols) {
    //Store dimensions that cols contains
    for (const col of cols) {
      let lastDims = [];
      for (const colItem of col) {
        if (rowDimensions.indexOf(colItem.dimension) > -1) {
          return {
            success: false,
            error: `${colItem.dimension} dimension in cols is duplicated`,
          };
        } else {
          //Check whether each col has same dimensions
          if (lastDims.indexOf(colItem.dimension) > -1)
            return {
              sucess: false,
              error: `${colItem.dimension} dimension in cols is duplicated!`,
            };
          else lastDims.push(colItem.dimension);
        }
      }
      colDimensions.push(...lastDims);
    }

    //Clean duplicated dimensions of cols
    colDimensions = colDimensions.filter((value, index, self) => {
      return self.indexOf(value) === index;
    });
  }

  if (filters) {
    //Store dimensions that filters contains
    for (const filter of filters) {
      let lastDims = [];
      for (const filterItem of filter) {
        if (
          colDimensions.indexOf(filterItem.dimension) > -1 ||
          rowDimensions.indexOf(filterItem.dimension) > -1
        ) {
          return {
            success: false,
            error: `${filterItem.dimension} dimension in filters is duplicated`,
          };
        } else {
          //Check whether each filter has same dimensions
          if (lastDims.indexOf(filterItem.dimension) > -1)
            return {
              sucess: false,
              error: `${filterItem.dimension} dimension in filters is duplicated!`,
            };
          else lastDims.push(filterItem.dimension);
        }
      }
    }
  }

  return { success: true };
};

/*
 *Function: Build query from row, col, filters
 */
const buildQuery = (rowItem, colItem, filters) => {
  let factCol = "sales";
  let factTable = "sales";
  // let query = `Select product.name, period.name, cutomer. From ${factTable} \nWhere 1 = 1`;
  let query = `Select ${colItem[0].aggregate}(${factCol}) as result From ${factTable} \nWhere 1 = 1`;

  for (const item of colItem)
    query += `\nAnd ${item.dimension}_id In (Select ${item.dimension}.id
      From ${item.dimension}
      Where ${item.dimension}.id In
      (
        Select ${item.dimension}_id
        From ${item.dimension}_map, ${item.dimension}
        Where ${item.dimension}_map.parent_${item.dimension}_id = ${item.dimension}.id and (${item.dimension}.name = '${item.member}' or ${item.dimension}.alias = '${item.member}')
      )
      Order By id)`;

  for (const item of rowItem)
    query += `\nAnd ${item.dimension}_id In (Select ${item.dimension}.id
      From ${item.dimension}
      Where ${item.dimension}.id In
      (
        Select ${item.dimension}_id
        From ${item.dimension}_map, ${item.dimension}
        Where ${item.dimension}_map.parent_${item.dimension}_id = ${item.dimension}.id and (${item.dimension}.name = '${item.member}' or ${item.dimension}.alias = '${item.member}')
      )
      Order By id)`;

  // if(filters){
  //   for (const filter of filters) {
  //     query += `\nAnd ${filter.dimension}_id In (Select ${filter.dimension}_id From ${filter.dimension}_map, ${filter.dimension} Where ${filter.dimension}_map.parent_${filter.dimension}_id=${filter.dimension}.id And (${filter.dimension}.name='${filter.member}' Or ${filter.dimension}.alias='${filter.member}'))`;
  //   }
  // }
  return query;
};

/*
 *Function: Concate spec information into former dimesion from array of dimensions
 */
const concateSpec = (parents) => {
  let concatedSpec = [];
  // console.log(parents)

  parents.forEach((parent) => {
    if (parent.length > 1) {
      for (let i = parent.length - 2; i >= 0; i--) {
        parent[i] = { ...parent[i], spec: parent[i + 1] };
      }
      concatedSpec.push({ ...parent[0] });
    } else {
      concatedSpec.push({ ...parent[0] });
    }
  });
  // console.log("concatedSPec*****************", concatedSpec)
  return concatedSpec;
};

//Make children from dimension using their relation information
const expandRelation = async (parent) => {
  const client = await pool.connect();
  let tmp = [];
  // console.log("makeChildren ", parent);
  const { dimension, member, relation, ...rest } = parent;
  let query;
  // console.log("makeChildren inner", parent, dimension, member, relation, rest);
  switch (relation) {
    case "Descendants":
      query = `Select ${dimension}.name
                  From ${dimension}
                  Where ${dimension}.id In
                  (
                    Select ${dimension}_id
                    From ${dimension}_map, ${dimension}
                    Where ${dimension}_map.parent_${dimension}_id = ${dimension}.id and (${dimension}.name = '${member}' or ${dimension}.alias = '${member}') and ${dimension}_map.${dimension}_id != ${dimension}_map.parent_${dimension}_id
                  )
                  Order By id`;
      break;
    case "IDescendants":
      query = `Select ${dimension}.name
                  From ${dimension}
                  Where ${dimension}.id In
                  (
                    Select ${dimension}_id
                    From ${dimension}_map, ${dimension}
                    Where ${dimension}_map.parent_${dimension}_id = ${dimension}.id and (${dimension}.name = '${member}' or ${dimension}.alias = '${member}')
                  )
                  Order By id`;
      break;
    case "Children":
      query = `Select ${dimension}.name
                  From ${dimension}
                  Where ${dimension}.parent_id In
                  (
                    Select id
                    From  ${dimension}
                    Where  (${dimension}.name = '${member}' or ${dimension}.alias = '${member}')
                  )
                  Order By id`;
      break;
    case "IChildren":
      query = `Select ${dimension}.name
                  From ${dimension}
                  Where ${dimension}.parent_id In
                  (
                    Select id
                    From  ${dimension}
                    Where  (${dimension}.name = '${member}' or ${dimension}.alias = '${member}')
                  )
                  or
                  ${dimension}.id In
                  (
                    Select id
                    From ${dimension}
                    Where (${dimension}.name = '${member}' or ${dimension}.alias = '${member}')
                  )
                  Order By id`;
      break;
    case "bottomLevel":
      query = `Select name
                  From
                  (Select *
                  From ${dimension}
                  Where ${dimension}.id In
                  (Select ${dimension}_id
                  From ${dimension}, ${dimension}_map
                  Where ${dimension}.id = ${dimension}_map.parent_${dimension}_id and (${dimension}.name = '${member}' or ${dimension}.alias = '${member}'))
                  and parent_id Is not null) as tmp
                  Where tmp.id not in 
                  (Select parent_id
                  From ${dimension}
                  Where ${dimension}.id In
                  (Select ${dimension}_id
                  From ${dimension}, ${dimension}_map
                  Where ${dimension}.id = ${dimension}_map.parent_${dimension}_id and (${dimension}.name = '${member}' or ${dimension}.alias = '${member}'))
                  and parent_id Is not null)
                  Order By id`;
      break;
    default:
      query = `Select ${dimension}.name
                  From ${dimension}
                  Where ${dimension}.name = '${member}' or ${dimension}.alias = '${member}'
                  Order By id`;
      break;
  }
  // console.log("In makeChildren query", query)
  let tmpResult = await client.query(query);
  tmpResult.rows.forEach((result) => {
    tmp = [...tmp, { dimension: dimension, member: result.name, ...rest }];
  });
  // console.log("In makeChildren tmp", tmp);
  // tmp = Array.from(new Set(tmp.map((item) => JSON.stringify(item)))).map(
  //   (item) => JSON.parse(item)
  // );
  client.end();
  return tmp;
};

const generateCombination = (parent) => {
  let result = [];
  const generateCombinationArray = (index, currentCombination) => {
    if (index === parent.length) {
      result.push(currentCombination);
      return;
    }
    for (let i = 0; i < parent[index].length; i++) {
      generateCombinationArray(
        index + 1,
        currentCombination.concat(parent[index][i])
      );
    }
  };

  generateCombinationArray(0, []);

  return result;
};

/*
 *Function: Concate spec information into former dimesion from array of dimensions
 */
const relationQuery = (parent) => {
  let result = [];

  const { dimension, member, relation, ...rest } = parent;
  let tmp = [];
  let query;
  // console.log("makeChildren inner", parent, dimension, member, relation, rest);
  switch (relation) {
    case "Descendants":
      query = `Select ${dimension}.id
                From ${dimension}
                Where ${dimension}.id In
                (
                  Select ${dimension}_id
                  From ${dimension}_map, ${dimension}
                  Where ${dimension}_map.parent_${dimension}_id = ${dimension}.id and (${dimension}.name = '${member}' or ${dimension}.alias = '${member}') and ${dimension}_map.${dimension}_id != ${dimension}_map.parent_${dimension}_id
                )
                Order By id`;
      break;
    case "IDescendants":
      query = `Select ${dimension}.id
                From ${dimension}
                Where ${dimension}.id In
                (
                  Select ${dimension}_id
                  From ${dimension}_map, ${dimension}
                  Where ${dimension}_map.parent_${dimension}_id = ${dimension}.id and (${dimension}.name = '${member}' or ${dimension}.alias = '${member}')
                )
                Order By id`;
      break;
    case "Children":
      query = `Select ${dimension}.id
                From ${dimension}
                Where ${dimension}.parent_id In
                (
                  Select id
                  From  ${dimension}
                  Where  (${dimension}.name = '${member}' or ${dimension}.alias = '${member}')
                )
                Order By id`;
      break;
    case "IChildren":
      query = `Select ${dimension}.id
                From ${dimension}
                Where ${dimension}.parent_id In
                (
                  Select id
                  From  ${dimension}
                  Where  (${dimension}.name = '${member}' or ${dimension}.alias = '${member}')
                )
                or
                ${dimension}.id In
                (
                  Select id
                  From ${dimension}
                  Where (${dimension}.name = '${member}' or ${dimension}.alias = '${member}')
                )
                Order By id`;
      break;
    case "bottomLevel":
      query = `Select id
                From
                (Select *
                From ${dimension}
                Where ${dimension}.id In
                (Select ${dimension}_id
                From ${dimension}, ${dimension}_map
                Where ${dimension}.id = ${dimension}_map.parent_${dimension}_id and (${dimension}.name = '${member}' or ${dimension}.alias = '${member}'))
                and parent_id Is not null) as tmp
                Where tmp.id not in 
                (Select parent_id
                From ${dimension}
                Where ${dimension}.id In
                (Select ${dimension}_id
                From ${dimension}, ${dimension}_map
                Where ${dimension}.id = ${dimension}_map.parent_${dimension}_id and (${dimension}.name = '${member}' or ${dimension}.alias = '${member}'))
                and parent_id Is not null)
                Order By id`;
      break;
    default:
      query = `Select ${dimension}.id
                From ${dimension}
                Where ${dimension}.name = '${member}' or ${dimension}.alias = '${member}'
                Order By id`;
      break;
  }

  return query;
};

/*
 *Function: Make children of spec dimension using  spec's relation information
 */
const expandSpec = async (parents) => {
  const expandedResults = [];
  const recursiveExpansion = async (item, parentData) => {
    // console.log("item, condition", !item)
    if (item === undefined) {
      // console.log("recursion ", parentData)
      return [parentData];
    }
    let specChildren = await makeChildren([item]);
    let expandedChildren = [];
    for (const specChild of specChildren[0]) {
      let newData = { ...parentData, spec: specChild };
      let result = await recursiveExpansion(item.spec, newData);
      expandedChildren.push(...result);
    }
    return expandedChildren;
  };

  for (const bridgeParents of parents) {
    let expandedBridge = [];
    for (const parent of bridgeParents) {
      if (parent.hasOwnProperty("spec")) {
        let expanded = await recursiveExpansion(parent.spec, {
          ...parent,
          spec: {},
        });
        expandedBridge.push(...expanded);
      } else {
        expandedBridge.push([parent]); // If no spec, add the parent as is
      }
    }
    expandedResults.push(expandedBridge);
  }

  return expandedResults;
};

/*
 *Function: Return leaves of request dimension
 */
const leaves = async (req, res) => {
  try {
    await setDBInfo();
    const client = await pool.connect();
    const { dimension, member } = req.body;
    let query = `Select name
                From
                (Select *
                From ${dimension}
                Where ${dimension}.id In
                (Select ${dimension}_id
                From ${dimension}, ${dimension}_map
                Where ${dimension}.id = ${dimension}_map.parent_${dimension}_id and (${dimension}.name = '${member}' or ${dimension}.alias = '${member}'))
                and parent_id Is not null) as tmp
                Where tmp.id not in 
                (Select parent_id
                From ${dimension}
                Where ${dimension}.id In
                (Select ${dimension}_id
                From ${dimension}, ${dimension}_map
                Where ${dimension}.id = ${dimension}_map.parent_${dimension}_id and (${dimension}.name = '${member}' or ${dimension}.alias = '${member}'))
                and parent_id Is not null)
                Order By id`;
    const result = await client.query(query);
    res.json(result.rows);
    client.end();
  } catch (error) {
    console.log(error);
  }
};

module.exports = {
  setDBInfo,
  mapData,
  queryCell,
  pivot,
  leaves,
  getTables,
  getTableData
};
