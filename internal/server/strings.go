package server

import (
	"strconv"

	"github.com/tidwall/redcon"
)

// Strings operations https://redis.io/commands/?group=string

func (s *Server) handleAPPEND(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 3 {
		conn.WriteError("ERR wrong number of arguments for 'APPEND' command")
		return
	}

	size, err := s.db.APPEND(cmd.Args[1], cmd.Args[2])
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	conn.WriteInt(size)
}

func (s *Server) handleDECR(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for 'DECR' command")
		return
	}

	val, err := s.db.DECR(cmd.Args[1])
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt64(val)
}

func (s *Server) handleDECRBY(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 3 {
		conn.WriteError("ERR wrong number of arguments for 'DECRBY' command")
		return
	}

	by, err := strconv.ParseInt(string(cmd.Args[2]), 10, 64)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	val, err := s.db.DECRBY(cmd.Args[1], int(by))
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt64(val)
}

func (s *Server) handleGET(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for 'GET' command")
		return
	}

	val, err := s.db.GET(cmd.Args[1])
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteString(string(val))
}

func (s *Server) handleGETDEL(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for 'GETDEL' command")
		return
	}

	val, err := s.db.GETDEL(cmd.Args[1])
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteString(string(val))
}

func (s *Server) handleGETRANGE(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 4 {
		conn.WriteError("ERR wrong number of arguments for 'GETRANGE' command")
		return
	}

	start, err := strconv.ParseInt(string(cmd.Args[2]), 10, 64)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	end, err := strconv.ParseInt(string(cmd.Args[3]), 10, 64)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	val, err := s.db.GETRANGE(cmd.Args[1], int(start), int(end))
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteString(string(val))
}

func (s *Server) handleGETSET(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 3 {
		conn.WriteError("ERR wrong number of arguments for 'GETSET' command")
		return
	}

	val, err := s.db.GETSET(cmd.Args[1], cmd.Args[2])
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteString(string(val))
}

func (s *Server) handleINCR(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for 'INCR' command")
		return
	}

	val, err := s.db.INCR(cmd.Args[1])
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt64(val)
}

func (s *Server) handleINCRBY(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 3 {
		conn.WriteError("ERR wrong number of arguments for 'INCRBY' command")
		return
	}

	by, err := strconv.ParseInt(string(cmd.Args[2]), 10, 64)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	val, err := s.db.INCRBY(cmd.Args[1], int(by))
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt64(val)
}

func (s *Server) handleINCRBYFLOAT(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 3 {
		conn.WriteError("ERR wrong number of arguments for 'INCRBYFLOAT' command")
		return
	}

	by, err := strconv.ParseFloat(string(cmd.Args[2]), 64)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	val, err := s.db.INCRBYFLOAT(cmd.Args[1], by)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteString(val)
}

func (s *Server) handleMGET(conn redcon.Conn, cmd redcon.Command) {
	res, err := s.db.MGET(cmd.Args[1:]...)
	if err != nil {
		conn.WriteError("ERR in 'MGET' command: " + err.Error())
		return
	}

	conn.WriteArray(len(res))
	for i := range res {
		conn.WriteBulkString(string(res[i]))
	}
}

func (s *Server) handleMSET(conn redcon.Conn, cmd redcon.Command) {
	if (len(cmd.Args)-1)%2 != 0 {
		conn.WriteError("ERR wrong number of arguments for 'MSET' command")
		return
	}

	err := s.db.MSET(cmd.Args[1:]...)
	if err != nil {
		conn.WriteError("ERR in 'MSET' command: " + err.Error())
		return
	}
	conn.WriteString("OK")
}

func (s *Server) handleSET(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 3 {
		conn.WriteError("ERR wrong number of arguments for 'SET' command")
		return
	}

	err := s.db.SET(cmd.Args[1], cmd.Args[2])
	if err != nil {
		conn.WriteError("ERR in 'SET' command: " + err.Error())
		return
	}
	conn.WriteString("OK")
}

func (s *Server) handleSTRLEN(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != 2 {
		conn.WriteError("ERR wrong number of arguments for 'STRLEN' command")
		return
	}

	val, err := s.db.STRLEN(cmd.Args[1])
	if err != nil {
		conn.WriteError("ERR in 'STRLEN' command: " + err.Error())
		return
	}
	conn.WriteInt64(val)
}
