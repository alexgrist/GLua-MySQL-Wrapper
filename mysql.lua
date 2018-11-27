--[[
	mysql - 1.0.3
	A simple MySQL wrapper for Garry's Mod.
	Alexander Grist-Hucker
	http://www.alexgrist.com
--]]

mysql = mysql or {};

local QueueTable = {};
local Module = "sqlite";
local Encoding = 'utf8'
local Connected = nil;
local isnumber, tostring, _table, _string, istable, isfunction, isstring = isnumber, tostring, table, string, istable, isfunction, isstring
local ErrorNoHalt = ErrorNoHalt

-- # Phrases
local MODULE_PREFIX = "[mysql]"
local MODULE_NOT_EXIST = "%s The %s module does not exist!\n";

-- # Begin Query Class.
local QUERY_CLASS = {};
QUERY_CLASS.__index = QUERY_CLASS;

function QUERY_CLASS:New(tableName, queryType)
	local newObject = setmetatable({}, QUERY_CLASS);
		newObject.queryType = queryType;
		newObject.tableName = tableName;
		newObject.selectList = {};
		newObject.insertList = {};
		newObject.updateList = {};
		newObject.createList = {};
		newObject.whereList = {};
		newObject.orderByList = {};
	return newObject;
end;

function QUERY_CLASS:Escape(text)
	return mysql:Escape(tostring(text));
end;

function QUERY_CLASS:ForTable(tableName)
	self.tableName = tableName;
end;

function QUERY_CLASS:Where(key, value)
	self:WhereEqual(key, value);
end;

function QUERY_CLASS:WhereEqual(key, value)
	self.whereList[#self.whereList + 1] = "`"..key.."` = \""..self:Escape(value).."\"";
end;

function QUERY_CLASS:WhereNotEqual(key, value)
	self.whereList[#self.whereList + 1] = "`"..key.."` != \""..self:Escape(value).."\"";
end;

function QUERY_CLASS:WhereLike(key, value)
	self.whereList[#self.whereList + 1] = "`"..key.."` LIKE \""..self:Escape(value).."\"";
end;

function QUERY_CLASS:WhereNotLike(key, value)
	self.whereList[#self.whereList + 1] = "`"..key.."` NOT LIKE \""..self:Escape(value).."\"";
end;

function QUERY_CLASS:WhereGT(key, value)
	self.whereList[#self.whereList + 1] = "`"..key.."` > \""..self:Escape(value).."\"";
end;

function QUERY_CLASS:WhereLT(key, value)
	self.whereList[#self.whereList + 1] = "`"..key.."` < \""..self:Escape(value).."\"";
end;

function QUERY_CLASS:WhereGTE(key, value)
	self.whereList[#self.whereList + 1] = "`"..key.."` >= \""..self:Escape(value).."\"";
end;

function QUERY_CLASS:WhereLTE(key, value)
	self.whereList[#self.whereList + 1] = "`"..key.."` <= \""..self:Escape(value).."\"";
end;

function QUERY_CLASS:OrderByDesc(key)
	self.orderByList[#self.orderByList + 1] = "`"..key.."` DESC";
end;

function QUERY_CLASS:OrderByAsc(key)
	self.orderByList[#self.orderByList + 1] = "`"..key.."` ASC";
end;

function QUERY_CLASS:Callback(queryCallback)
	self.callback = queryCallback;
end;

function QUERY_CLASS:Select(fieldName)
	self.selectList[#self.selectList + 1] = "`"..fieldName.."`";
end;

function QUERY_CLASS:Insert(key, value)
	self.insertList[#self.insertList + 1] = {"`"..key.."`", "\""..self:Escape(value).."\""};
end;

function QUERY_CLASS:Update(key, value)
	self.updateList[#self.updateList + 1] = {"`"..key.."`", "\""..self:Escape(value).."\""};
end;

function QUERY_CLASS:Create(key, value)
	self.createList[#self.createList + 1] = {"`"..key.."`", value};
end;

function QUERY_CLASS:PrimaryKey(key)
	self.primaryKey = "`"..key.."`";
end;

function QUERY_CLASS:Limit(value)
	self.limit = value;
end;

function QUERY_CLASS:Offset(value)
	self.offset = value;
end;

local function BuildSelectQuery(queryObj)
	local queryString = {"SELECT"};

	if (!istable(queryObj.selectList) or #queryObj.selectList == 0) then
		queryString[#queryString + 1] = " *";
	else
		queryString[#queryString + 1] = " ".._table.concat(queryObj.selectList, ", ");
	end;

	if (isstring(queryObj.tableName)) then
		queryString[#queryString + 1] = " FROM `"..queryObj.tableName.."` ";
	else
		ErrorNoHalt(""..MODULE_PREFIX.." No table name specified!\n");
		return;
	end;

	if (istable(queryObj.whereList) and #queryObj.whereList > 0) then
		queryString[#queryString + 1] = " WHERE ";
		queryString[#queryString + 1] = _table.concat(queryObj.whereList, " AND ");
	end;

	if (istable(queryObj.orderByList) and #queryObj.orderByList > 0) then
		queryString[#queryString + 1] = " ORDER BY ";
		queryString[#queryString + 1] = _table.concat(queryObj.orderByList, ", ");
	end;

	if (isnumber(queryObj.limit)) then
		queryString[#queryString + 1] = " LIMIT ";
		queryString[#queryString + 1] = queryObj.limit;
	end;

	return _table.concat(queryString);
end;

local function BuildInsertQuery(queryObj)
	local queryString = {"INSERT INTO"};
	local keyList = {};
	local valueList = {};

	if (isstring(queryObj.tableName)) then
		queryString[#queryString + 1] = " `"..queryObj.tableName.."`";
	else
		ErrorNoHalt(""..MODULE_PREFIX.." No table name specified!\n");
		return;
	end;

	for i = 1, #queryObj.insertList do
		keyList[#keyList + 1] = queryObj.insertList[i][1];
		valueList[#valueList + 1] = queryObj.insertList[i][2];
	end;

	if (#keyList == 0) then
		return;
	end;

	queryString[#queryString + 1] = " (".._table.concat(keyList, ", ")..")";
	queryString[#queryString + 1] = " VALUES (".._table.concat(valueList, ", ")..")";

	return _table.concat(queryString);
end;

local function BuildUpdateQuery(queryObj)
	local queryString = {"UPDATE"};

	if (isstring(queryObj.tableName)) then
		queryString[#queryString + 1] = " `"..queryObj.tableName.."`";
	else
		ErrorNoHalt(""..MODULE_PREFIX.." No table name specified!\n");
		return;
	end;

	if (istable(queryObj.updateList) and #queryObj.updateList > 0) then
		local updateList = {};

		queryString[#queryString + 1] = " SET";

		for i = 1, #queryObj.updateList do
			updateList[#updateList + 1] = queryObj.updateList[i][1].." = "..queryObj.updateList[i][2];
		end;

		queryString[#queryString + 1] = " ".._table.concat(updateList, ", ");
	end;

	if (istable(queryObj.whereList) and #queryObj.whereList > 0) then
		queryString[#queryString + 1] = " WHERE ";
		queryString[#queryString + 1] = _table.concat(queryObj.whereList, " AND ");
	end;

	if (isnumber(queryObj.offset)) then
		queryString[#queryString + 1] = " OFFSET ";
		queryString[#queryString + 1] = queryObj.offset;
	end;

	return _table.concat(queryString);
end;

local function BuildDeleteQuery(queryObj)
	local queryString = {"DELETE FROM"}

	if (isstring(queryObj.tableName)) then
		queryString[#queryString + 1] = " `"..queryObj.tableName.."`";
	else
		ErrorNoHalt(""..MODULE_PREFIX.." No table name specified!\n");
		return;
	end;

	if (istable(queryObj.whereList) and #queryObj.whereList > 0) then
		queryString[#queryString + 1] = " WHERE ";
		queryString[#queryString + 1] = _table.concat(queryObj.whereList, " AND ");
	end;

	if (isnumber(queryObj.limit)) then
		queryString[#queryString + 1] = " LIMIT ";
		queryString[#queryString + 1] = queryObj.limit;
	end;

	return _table.concat(queryString);
end;

local function BuildDropQuery(queryObj)
	local queryString = {"DROP TABLE"}

	if (isstring(queryObj.tableName)) then
		queryString[#queryString + 1] = " `"..queryObj.tableName.."`";
	else
		ErrorNoHalt(""..MODULE_PREFIX.." No table name specified!\n");
		return;
	end;

	return _table.concat(queryString);
end;

local function BuildTruncateQuery(queryObj)
	local queryString = {"TRUNCATE TABLE"}

	if (isstring(queryObj.tableName)) then
		queryString[#queryString + 1] = " `"..queryObj.tableName.."`";
	else
		ErrorNoHalt(""..MODULE_PREFIX.." No table name specified!\n");
		return;
	end;

	return _table.concat(queryString);
end;

local function BuildCreateQuery(queryObj)
	local queryString = {"CREATE TABLE IF NOT EXISTS"};

	if (isstring(queryObj.tableName)) then
		queryString[#queryString + 1] = " `"..queryObj.tableName.."`";
	else
		ErrorNoHalt(""..MODULE_PREFIX.." No table name specified!\n");
		return;
	end;

	queryString[#queryString + 1] = " (";

	if (istable(queryObj.createList) and #queryObj.createList > 0) then
		local createList = {};

		for i = 1, #queryObj.createList do
			if (Module == "sqlite") then
				createList[#createList + 1] = queryObj.createList[i][1].." ".._string.gsub(_string.gsub(_string.gsub(queryObj.createList[i][2], "AUTO_INCREMENT", ""), "AUTOINCREMENT", ""), "INT ", "INTEGER ");
			else
				createList[#createList + 1] = queryObj.createList[i][1].." "..queryObj.createList[i][2];
			end;
		end;

		queryString[#queryString + 1] = " ".._table.concat(createList, ", ");
	end;

	if (isstring(queryObj.primaryKey)) then
		queryString[#queryString + 1] = ", PRIMARY KEY";
		queryString[#queryString + 1] = " ("..queryObj.primaryKey..")";
	end;

	queryString[#queryString + 1] = " )";

	return _table.concat(queryString); 
end;

function QUERY_CLASS:Execute(bQueueQuery)
	local queryString = nil;
	local queryType = _string.lower(self.queryType);

	if (queryType == "select") then
		queryString = BuildSelectQuery(self);
	elseif (queryType == "insert") then
		queryString = BuildInsertQuery(self);
	elseif (queryType == "update") then
		queryString = BuildUpdateQuery(self);
	elseif (queryType == "delete") then
		queryString = BuildDeleteQuery(self);
	elseif (queryType == "drop") then
		queryString = BuildDropQuery(self);
	elseif (queryType == "truncate") then
		queryString = BuildTruncateQuery(self);
	elseif (queryType == "create") then
		queryString = BuildCreateQuery(self);
	end;

	if (isstring(queryString)) then
		if (!bQueueQuery) then
			return mysql:RawQuery(queryString, self.callback);
		else
			return mysql:Queue(queryString, self.callback);
		end;
	end;
end;

--[[
	End Query Class.
--]]

function mysql:Select(tableName)
	return QUERY_CLASS:New(tableName, "SELECT");
end;

function mysql:Insert(tableName)
	return QUERY_CLASS:New(tableName, "INSERT");
end;

function mysql:Update(tableName)
	return QUERY_CLASS:New(tableName, "UPDATE");
end;

function mysql:Delete(tableName)
	return QUERY_CLASS:New(tableName, "DELETE");
end;

function mysql:Drop(tableName)
	return QUERY_CLASS:New(tableName, "DROP");
end;

function mysql:Truncate(tableName)
	return QUERY_CLASS:New(tableName, "TRUNCATE");
end;

function mysql:Create(tableName)
	return QUERY_CLASS:New(tableName, "CREATE");
end;

-- A function to connect to the MySQL database.
function mysql:Connect(host, username, password, database, port, socket, flags)
	if (!port) then
		port = 3306; -- # [pg: 5432]
	end;

	if (Module == "tmysql4") then
		if (!istable(tmysql)) then
			require("tmysql4");
		end;

		if (tmysql) then
			local errorText = nil;

			self.connection, errorText = tmysql.initialize(host, username, password, database, port, socket, flags);

			if (!self.connection) then
				self:OnConnectionFailed(errorText);
			else
				self:OnConnected();
			end;
		else
			ErrorNoHalt(_string.format(MODULE_NOT_EXIST, MODULE_PREFIX, Module));
		end;
	elseif (Module == "mysqloo") then
		if (!istable(mysqloo)) then
			require("mysqloo");
		end;
	
		if (mysqloo) then
			local clientFlag = flags or 0;

			if (!isstring(socket)) then
				self.connection = mysqloo.connect(host, username, password, database, port);
			else
				self.connection = mysqloo.connect(host, username, password, database, port, socket, clientFlag);
			end;

			self.connection.onConnected = function(database)
				local success, err = database:setCharacterSet(Encoding)
				if !success then
					ErrorNoHalt(_string.format("%s Failed to set connection encoding!\n%s\n", MODULE_PREFIX, err));
				end
				
				self:OnConnected();
			end;

			self.connection.onConnectionFailed = function(database, errorText)
				self:OnConnectionFailed(errorText);
			end;		

			self.connection:connect();
			
			-- # ping it every 30 seconds to make sure we're not losing connection
			timer.Create("Mysqloo#keep_alive", 30, 0, function()
				self.connection:ping()
			end)
		else
			ErrorNoHalt(_string.format(MODULE_NOT_EXIST, Module));
		end;
	elseif (Module == "postgresql") then
		if (!istable(pg)) then
			require("pg");
		end;
		
		if (pg) then
			self.connection = pg.new_connection()
			local success, err = self.connection:connect(host, username, password, database, port)
			if success then
				success, err = self.connection:set_encoding(Encoding)
				if !success then
					ErrorNoHalt(_string.format("%s Failed to set connection encoding!\n%s\n", MODULE_PREFIX, err));
				end
				
				self:OnConnected();
			else
				self:OnConnectionFailed(err);
			end
		else
			ErrorNoHalt(_string.format(MODULE_NOT_EXIST, Module));
		end
	elseif (Module == "sqlite") then
		self:OnConnected();
	end;
end;

-- A function to query the MySQL database.
function mysql:RawQuery(query, callback, flags, ...)
	if (!self.connection and Module != "sqlite") then
		self:Queue(query);
	end;

	if (Module == "tmysql4") then
		local queryFlag = flags or QUERY_FLAG_ASSOC;

		self.connection:Query(query, function(result)
			local queryStatus = result[1]["status"];

			if (queryStatus) then
				if (isfunction(callback)) then
					local bStatus, value = pcall(callback, result[1]["data"], queryStatus, result[1]["lastid"]);

					if (!bStatus) then
						ErrorNoHalt(_string.format("%s MySQL Callback Error!\n%s\n", MODULE_PREFIX, value));
					end;
				end;
			else
				ErrorNoHalt(_string.format("%s MySQL Query Error!\nQuery: %s\n%s\n", MODULE_PREFIX, query, result[1]["error"]));
			end;
		end, queryFlag, ...);
	elseif (Module == "mysqloo") then
		local queryObj = self.connection:query(query);

		queryObj:setOption(mysqloo.OPTION_NAMED_FIELDS);
		queryObj.onSuccess = function(queryObj, result)
			if (callback) then
				local bStatus, value = pcall(callback, result, query, queryObj:lastInsert());

				if (!bStatus) then
					ErrorNoHalt(_string.format("%s MySQL Callback Error!\n%s\n", MODULE_PREFIX, value));
				end;
			end;
		end;

		queryObj.onError = function(queryObj, errorText)
			ErrorNoHalt(_string.format("%s MySQL Query Error!\nQuery: %s\n%s\n", MODULE_PREFIX, query, errorText));
		end;

		queryObj:start();
	elseif (Module == "postgresql") then
		local queryObj = self.connection:query(query);
		local qStart = os.clock()

		queryObj:on("success", function(result, size)
			if (callback) then
				local bStatus, value = pcall(callback, result, query, math.Round(os.clock() - qStart, 3));

				if (!bStatus) then
					ErrorNoHalt(_string.format("%s PostgreSQL Callback Error!\n%s\n", MODULE_PREFIX, value));
				end;
			end;
		end)

		queryObj:on("error", function(errorText)
			ErrorNoHalt(_string.format("%s PostgreSQL Query Error!\nQuery: %s\n%s\n", MODULE_PREFIX, query, errorText));
		end)

		queryObj:set_sync(false)
		queryObj:run()
		
		qStart = nil
	elseif (Module == "sqlite") then
		local result = sql.Query(query);

		if (result == false) then
			ErrorNoHalt(_string.format("%s SQL Query Error!\nQuery: %s\n%s\n", MODULE_PREFIX, query, sql.LastError()));
		else
			if (callback) then
				local bStatus, value = pcall(callback, result);

				if (!bStatus) then
					ErrorNoHalt(_string.format("%s SQL Callback Error!\n%s\n", MODULE_PREFIX, value));
				end;
			end;
		end;
	else
		ErrorNoHalt(_string.format("%s Unsupported module \"%s\"!\n", MODULE_PREFIX, Module));
	end;
end;

-- A function to add a query to the queue.
function mysql:Queue(queryString, callback)
	if (isstring(queryString)) then
		QueueTable[#QueueTable + 1] = {queryString, callback};
	end;
end;

-- A function to escape a string for MySQL.
function mysql:Escape(text)
	if (self.connection) then
		if (Module == "tmysql4") then
			return self.connection:Escape(text);
		elseif (Module == "mysqloo" or Module == "postgresql") then
			return self.connection:escape(text);
		end
	else
		return sql.SQLStr(_string.gsub(text, "\"", "'"), true);
	end;
end;

-- A function to disconnect from the MySQL database.
function mysql:Disconnect()
	if (self.connection) then
		if (Module == "tmysql4") then
			self.connection:Disconnect();	
		elseif (Module == "mysqloo") then
			self.connection:disconnect(true);
		elseif (Module == "postgresql") then
			self.connection:disconnect();
		end;
	end;

	Connected = nil;
	self.connection = nil;
end;

function mysql:Think()
	if (#QueueTable > 0) then
		if (istable(QueueTable[1])) then
			local queueObj = QueueTable[1];
			local queryString = queueObj[1];
			
			if (isstring(queryString)) then
				self:RawQuery(queryString, queueObj[2]);
			end;

			_table.remove(QueueTable, 1);
		end;
	end;
end;

-- A function to set the module that should be used.
function mysql:SetModule(moduleName)
	Module = moduleName;
end;

-- Called when the database connects sucessfully.
function mysql:OnConnected()
	Connected = true;
	MsgC(Color(25, 235, 25), ""..MODULE_PREFIX.." Connected to the database using "..Module.."!\n")
	hook.Run("DatabaseConnected");
end;

-- Called when the database connection fails.
function mysql:OnConnectionFailed(errorText)
	ErrorNoHalt(""..MODULE_PREFIX.." Unable to connect to the database!\n"..errorText.."\n");
	hook.Run("DatabaseConnectionFailed", errorText);
end;

-- A function to check whether or not the module is connected to a database.
function mysql:IsConnected()
	return Connected;
end;

return mysql;