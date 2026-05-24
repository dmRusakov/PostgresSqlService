package postgressql

type Config struct {
	Host     string
	Port     string
	User     string
	Password string
	DB       string
}

type Order struct {
	By    string `json:"by"`    // OrderBy is the field name (form Struct) to order by
	Dir   string `json:"dir"`   // OrderDir is the direction of the order, can be "asc" or "desc"
	Page  uint64 `json:"page"`  // Page is the page number for pagination, starting from 1
	Limit uint64 `json:"limit"` // PerPage is the number of items per page for pagination
}
