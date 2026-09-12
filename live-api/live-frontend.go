package live_api

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed static
var embedded embed.FS
var staticFE fs.FS

// strip prefix before serve
func init() {
	var err error
	staticFE, err = fs.Sub(embedded, "static")
	if err != nil {
		panic(err)
	}

}

func Static() http.Handler {
	return http.FileServer(http.FS(staticFE))
}
