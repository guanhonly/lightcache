package lightCache

import (
	"reflect"
	"testing"
)

func TestWriteAndGetOnCache(t *testing.T) {
	t.Parallel()
	type args struct {
		setKey   string
		setValue []byte
		getKey   string
	}
	tests := []struct {
		name      string
		args      args
		wantErr   bool
		wantEqual bool
	}{
		{
			name: "normal",
			args: args{
				setKey:   "key",
				setValue: []byte("value"),
				getKey:   "key",
			},
			wantErr:   false,
			wantEqual: true,
		},
		{
			name: "get another",
			args: args{
				setKey:   "key",
				setValue: []byte("value"),
				getKey:   "anotherKey",
			},
			wantErr:   false,
			wantEqual: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache := DefaultCacheBuilder().Build()
			err := cache.Set(tt.args.setKey, tt.args.setValue)
			if (err != nil) != tt.wantErr {
				t.Errorf("error: %v, wantErr: %v", err, tt.wantErr)
			}
			got, _ := cache.Get(tt.args.getKey)
			if reflect.DeepEqual(tt.args.setValue, got) != tt.wantEqual {
				t.Errorf("got: %v, wantEqual: %v", got, tt.wantEqual)
			}
		})
	}
}
