package dbclient

import "time"

/*
jdbc:mysql://localhost:3306/sillyhat_xu_db?serverTimezone=Asia/Singapore&useLegacyDatetimeCode=false&useUnicode=true&allowMultiQueries=true&characterEncoding=UTF-8&autoReconnect=true&zeroDateTimeBehavior=convertToNull&connectTimeout=2000&sessionVariables=character_set_connection=utf8mb4,character_set_client=utf8mb4
*/
//https://github.com/go-sql-driver/mysql
type Config struct {
	driverName              string
	userName                string
	password                string
	host                    string
	port                    int
	schema                  string
	ddlPath                 string
	flyway                  bool
	allowAllFiles           bool
	allowCleartextPasswords bool
	allowNativePasswords    bool
	allowOldPasswords       bool
	charset                 string
	collation               string
	clientFoundRows         bool
	columnsWithAlias        bool
	interpolateParams       bool
	loc                     string
	maxAllowedPacket        int64
	multiStatements         bool
	parseTime               bool
	readTimeout             time.Duration
	rejectReadOnly          bool
	serverPubKey            *string
	timeout                 time.Duration
	tls                     bool
	writeTimeout            time.Duration
	maxIdleConns            int
	maxOpenConns            int
	connMaxLifetime         time.Duration
}

type Option func(*Config)

func DriverName(driverName string) Option {
	return func(c *Config) {
		c.driverName = driverName
	}
}

func UserName(userName string) Option {
	return func(c *Config) {
		c.userName = userName
	}
}

func Password(password string) Option {
	return func(c *Config) {
		c.password = password
	}
}

func Host(host string) Option {
	return func(c *Config) {
		c.host = host
	}
}

func Port(port int) Option {
	return func(c *Config) {
		c.port = port
	}
}

func Schema(schema string) Option {
	return func(c *Config) {
		c.schema = schema
	}
}

// allowAllFiles=true disables the file allowlist for LOAD DATA LOCAL INFILE and allows all files.
// Might be insecure!
func AllowAllFiles(allowAllFiles bool) Option {
	return func(c *Config) {
		c.allowAllFiles = allowAllFiles
	}
}

// allowCleartextPasswords=true
// allows using the cleartext client side plugin if required by an account,
// such as one defined with the PAM authentication plugin.
// Sending passwords in clear text may be a security problem in some configurations.
// To avoid problems if there is any possibility that the password would be intercepted,
// clients should connect to MySQL Server using a method that protects the password.
// Possibilities include TLS / SSL, IPsec, or a private network.
func AllowCleartextPasswords(allowCleartextPasswords bool) Option {
	return func(c *Config) {
		c.allowCleartextPasswords = allowCleartextPasswords
	}
}

// allowNativePasswords=false disallows the usage of MySQL native password method.
func AllowNativePasswords(allowNativePasswords bool) Option {
	return func(c *Config) {
		c.allowNativePasswords = allowNativePasswords
	}
}

// allowOldPasswords=true allows the usage of the insecure old password method.
// This should be avoided, but is necessary in some cases. See also the old_passwords wiki page.
func AllowOldPasswords(allowOldPasswords bool) Option {
	return func(c *Config) {
		c.allowOldPasswords = allowOldPasswords
	}
}

// Sets the charset used for client-server interaction ("SET NAMES <value>").
// If multiple charsets are set (separated by a comma), the following charset is used if setting the charset failes.
// This enables for example support for utf8mb4 (introduced in MySQL 5.5.3) with fallback to utf8 for older servers (charset=utf8mb4,utf8).
// Usage of the charset parameter is discouraged because it issues additional queries to the server.
// Unless you need the fallback behavior, please use collation instead.
func Charset(charset string) Option {
	return func(c *Config) {
		c.charset = charset
	}
}

// Sets the collation used for client-server interaction on connection.
// In contrast to charset, collation does not issue additional queries.
// If the specified collation is unavailable on the target server, the connection will fail.
// A list of valid charsets for a server is retrievable with SHOW COLLATION.
// The default collation (utf8mb4_general_ci) is supported from MySQL 5.5.
// You should use an older collation (e.g. utf8_general_ci) for older MySQL.
// Collations for charset "ucs2", "utf16", "utf16le", and "utf32" can not be used (ref).
func Collation(collation string) Option {
	return func(c *Config) {
		c.collation = collation
	}
}

// clientFoundRows=true causes an UPDATE to return the number of matching rows instead of the number of rows changed.
func ClientFoundRows(clientFoundRows bool) Option {
	return func(c *Config) {
		c.clientFoundRows = clientFoundRows
	}
}

// When columnsWithAlias is true, calls to sql.Rows.Columns() will return the table alias and the column name separated by a dot.
// For example:
// SELECT u.id FROM users as u
// will return u.id instead of just id if columnsWithAlias=true.
func ColumnsWithAlias(columnsWithAlias bool) Option {
	return func(c *Config) {
		c.columnsWithAlias = columnsWithAlias
	}
}

// If interpolateParams is true,
// placeholders (?) in calls to db.Query() and db.Exec() are interpolated into a single query string with given parameters.
// This reduces the number of roundtrips, since the driver has to prepare a statement,
// execute it with given parameters and close the statement again with interpolateParams=false.
// This can not be used together with the multibyte encodings BIG5, CP932, GB2312, GBK or SJIS.
// These are rejected as they may introduce a SQL injection vulnerability!
func InterpolateParams(interpolateParams bool) Option {
	return func(c *Config) {
		c.interpolateParams = interpolateParams
	}
}

// Sets the location for time.Time values (when using parseTime=true).
// "Local" sets the system's location. See time.LoadLocation for details.
// Note that this sets the location for time.Time values but does not change MySQL's time_zone setting.
// For that see the time_zone system variable, which can also be set as a DSN parameter.
// Please keep in mind, that param values must be url.QueryEscape'ed.
// Alternatively you can manually replace the / with %2F. For example US/Pacific would be loc=US%2FPacific.
func Loc(loc string) Option {
	return func(c *Config) {
		c.loc = loc
	}
}

// Max packet size allowed in bytes.
// The default value is 4 MiB and should be adjusted to match the server settings.
// maxAllowedPacket=0 can be used to automatically fetch the max_allowed_packet variable from server on every connection.
func MaxAllowedPacket(maxAllowedPacket int64) Option {
	return func(c *Config) {
		c.maxAllowedPacket = maxAllowedPacket
	}
}

// Allow multiple statements in one query.
// While this allows batch queries, it also greatly increases the risk of SQL injections.
// Only the result of the first query is returned, all other results are silently discarded.
// When multiStatements is used, ? parameters must only be used in the first statement.
func MultiStatements(multiStatements bool) Option {
	return func(c *Config) {
		c.multiStatements = multiStatements
	}
}

// parseTime=true changes the output type of DATE and DATETIME values to time.Time instead of []byte / string The date or datetime like 0000-00-00 00:00:00 is converted into zero value of time.Time.
func ParseTime(parseTime bool) Option {
	return func(c *Config) {
		c.parseTime = parseTime
	}
}

// I/O read timeout. The value must be a decimal number with a unit suffix ("ms", "s", "m", "h"), such as "30s", "0.5m" or "1m30s".
func ReadTimeout(readTimeout time.Duration) Option {
	return func(c *Config) {
		c.readTimeout = readTimeout
	}
}

// rejectReadOnly=true causes the driver to reject read-only connections.
// This is for a possible race condition during an automatic failover, where the mysql client gets connected to a read-only replica after the failover.
// Note that this should be a fairly rare case,
// as an automatic failover normally happens when the primary is down, and the race condition shouldn't happen unless it comes back up online as soon as the failover is kicked off.
// On the other hand, when this happens, a MySQL application can get stuck on a read-only connection until restarted. It is however fairly easy to reproduce,
// for example, using a manual failover on AWS Aurora's MySQL-compatible cluster.
// If you are not relying on read-only transactions to reject writes that aren't supposed to happen,
// setting this on some MySQL providers (such as AWS Aurora) is safer for failovers.
// Note that ERROR 1290 can be returned for a read-only server and this option will cause a retry for that error.
// However the same error number is used for some other cases.
// You should ensure your application will never cause an ERROR 1290 except for read-only mode when enabling this option.
func RejectReadOnly(rejectReadOnly bool) Option {
	return func(c *Config) {
		c.rejectReadOnly = rejectReadOnly
	}
}

// Server public keys can be registered with mysql.RegisterServerPubKey,
// which can then be used by the assigned name in the DSN. Public keys are used to transmit encrypted data,
// e.g. for authentication. If the server's public key is known,
// it should be set manually to avoid expensive and potentially insecure transmissions of the public key from the server to the client each time it is required.
func ServerPubKey(serverPubKey string) Option {
	return func(c *Config) {
		c.serverPubKey = &serverPubKey
	}
}

// Timeout for establishing connections, aka dial timeout.
// The value must be a decimal number with a unit suffix ("ms", "s", "m", "h"), such as "30s", "0.5m" or "1m30s".
func Timeout(timeout time.Duration) Option {
	return func(c *Config) {
		c.timeout = timeout
	}
}

// tls=true enables TLS / SSL encrypted connection to the server.
// Use skip-verify if you want to use a self-signed or invalid certificate (server side) or use preferred to use TLS only when advertised by the server.
// This is similar to skip-verify, but additionally allows a fallback to a connection which is not encrypted. Neither skip-verify nor preferred add any reliable security. You can use a custom TLS config after registering it with mysql.RegisterTLSConfig.
func TLS(tls bool) Option {
	return func(c *Config) {
		c.tls = tls
	}
}

// I/O write timeout. The value must be a decimal number with a unit suffix ("ms", "s", "m", "h"), such as "30s", "0.5m" or "1m30s".
func WriteTimeout(writeTimeout time.Duration) Option {
	return func(c *Config) {
		c.writeTimeout = writeTimeout
	}
}

func MaxIdleConns(maxIdleConns int) Option {
	return func(c *Config) {
		c.maxIdleConns = maxIdleConns
	}
}

func MaxOpenConns(maxOpenConns int) Option {
	return func(c *Config) {
		c.maxOpenConns = maxOpenConns
	}
}

func ConnMaxLifetime(connMaxLifetime time.Duration) Option {
	return func(c *Config) {
		c.connMaxLifetime = connMaxLifetime
	}
}
