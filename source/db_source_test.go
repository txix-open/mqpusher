package source

import (
	"testing"
)

func TestAppendIDModClauseToQuery(t *testing.T) {
	type args struct {
		query    string
		mod      int
		equalsTo int
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "no where", args: args{
				query:    "SELECT id from something",
				mod:      5,
				equalsTo: 4,
			}, want: "SELECT id FROM something WHERE id % 5 = 4", wantErr: false},
		{
			name: "append to where", args: args{
				query:    "SELECT id from something WHERE 5 > 0",
				mod:      5,
				equalsTo: 4,
			}, want: "SELECT id FROM something WHERE 5 > 0 AND id % 5 = 4", wantErr: false},
		{
			name: "append to where with and", args: args{
				query:    "SELECT id from something WHERE 5 > 0 AND id > 2",
				mod:      5,
				equalsTo: 4,
			}, want: "SELECT id FROM something WHERE 5 > 0 AND id > 2 AND id % 5 = 4", wantErr: false},
		{
			name: "append to where with date", args: args{
				query:    "SELECT id, updated_at from something WHERE updated_at > '2019-10-15 09:28'",
				mod:      5,
				equalsTo: 4,
			}, want: "SELECT id,updated_at FROM something WHERE updated_at > '2019-10-15 09:28' AND id % 5 = 4", wantErr: false},
		{
			name: "append to where with date and group by", args: args{
				query:    "SELECT some_type, count(id) FROM something WHERE updated_at > '2019-10-15 09:28' group by some_type",
				mod:      2,
				equalsTo: 0,
			}, want: "SELECT some_type,COUNT(id) FROM something WHERE updated_at > '2019-10-15 09:28' AND id % 2 = 0 GROUP BY some_type", wantErr: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := AppendIDModClauseToQuery(tt.args.query, tt.args.mod, tt.args.equalsTo)
			if (err != nil) != tt.wantErr {
				t.Errorf("AppendIDModClauseToQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("AppendIDModClauseToQuery() got = %v, want %v", got, tt.want)
			}
		})
	}
}
