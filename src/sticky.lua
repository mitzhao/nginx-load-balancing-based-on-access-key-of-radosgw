local shared = ngx.shared
local lock = require "lock"
local var = ngx.var	
local log = ngx.log

local trim = function (s)
		return (s:gsub("^%s*(.-)%s*$", "%1"))
end

local ok, upstream = pcall(require, "ngx.upstream")
if not ok then
	error("ngx_upstream_lua module required")
end
local ok, upstream = pcall(require, "ngx.upstream")
if not ok then
	error("ngx_upstream_lua module required")
end
local ok, new_tab = pcall(require, "table.new")
if not ok or type(new_tab) ~= "function" then
	new_tab = function (narr, nrec) return {} end
end

local ERR = ngx.ERR
local INFO = ngx.INFO
local WARN = ngx.WARN
local DEBUG = ngx.DEBUG
local function info(...)
    log(INFO, "sticky: ", ...)
end

local function warn(...)
    log(WARN, "sticky: ", ...)
end

local function errlog(...)
    log(ERR, "sticky: ", ...)
end

local set_peer_down = upstream.set_peer_down
local get_primary_peers = upstream.get_primary_peers
local get_backup_peers = upstream.get_backup_peers
local get_upstreams = upstream.get_upstreams
local name = ngx.var.arg_name or "Anonymous"

	
	
local defaults = {
           store      = var.session_shm_store or "sessions",
		   uselocking = var.session_shm_uselocking or true,
           lock = {
                exptime  = tonumber(var.session_shm_lock_exptime)  or 30,
                timeout  = tonumber(var.session_shm_lock_timeout)  or 5,
                step     = tonumber(var.session_shm_lock_step)     or 0.001,
                ratio    = tonumber(var.session_shm_lock_ratio)    or 2,
                max_step = tonumber(var.session_shm_lock_max_step) or 0.5,
            }
        }

		
local _M = {
    _VERSION = '0.01',
}

local mt = { __index = _M }


function _M.new(opts)
	local c = defaults
	local m = defaults.store
	local l = defaults.uselocking
	local method = "random"
	if opts and opts.method then
			method = opts.method 
	end
	local self = {
        store      = shared[m],
        uselocking = l,
		method = method
    }
    if l then
        local x = c.lock or defaults.lock
        local s = {
            exptime  = tonumber(x.exptime)  or defaults.exptime,
            timeout  = tonumber(x.timeout)  or defaults.timeout,
            step     = tonumber(x.step)     or defaults.step,
            ratio    = tonumber(x.ratio)    or defaults.ratio,
            max_step = tonumber(x.max_step) or defaults.max_step
        }
        self.lock = lock:new(m, s)
    end
    return setmetatable(self, mt)
end



function is_in(servers, server )
	for i = 1 , #servers do
		if server == servers[i] then
			return true
		end
	end
	return false
end

function random_choose(servers)
	local server_num = math.random(#servers)
	return servers[server_num]
end

function roundrobin(servers)
	return nil
end

function choose_server(servers, method)
	if method == "random" then
		return random_choose(servers)
	elseif method == "roundrobin" then
		return roundrobin(servers)
	else
		return nil
	end
end

function _M:get(upstream, lifetime)
	local ngx_req = ngx.req
	local ngx_req_get_headers = ngx_req.get_headers
	local peers, err = get_primary_peers(upstream)
	if not peers then
		errlog("error get_primary_peers, ", upstream, "  ", err)
	    return nil
	end
	local headers = ngx_req_get_headers()
	local auth = headers["authorization"]
	local ak = nil
	if auth then
		local i,j = string.find(auth, "AWS")
		ak = string.sub(auth, j+1)
		i , j = string.find(ak, ":")
		ak = string.sub(ak, 0, i-1 )
		ak = trim(ak)
	else 
		ak = "anonymous"
	end
	info("user is ", ak)
	local idx = 1
	local alive_idx = 1
	local npeers = #peers
	local alive_servers = new_tab(10, 0)

	for i = 1 , npeers do
		local peer = peers[i]
		if not peer.down then
			alive_servers[alive_idx] = peer.name
			alive_idx = alive_idx + 1
		end
		idx = idx + 1
	end

	if not #alive_servers then
		errorlog("there is no alive_servers")
		return nil
	end

	local server = self.store:get(ak)
	local new_server = false
	if  not server then
		info("no server")
		new_server = true
	elseif not is_in(alive_servers, server) then
		info("old server is died")
		new_server = true
	end

	if new_server then
		server = choose_server(alive_servers, self.method)
		info("choose a new server : ", server)
	end

	self.store:set(ak, server, lifetime)
	return server
end

return _M

