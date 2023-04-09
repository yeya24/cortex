package storegateway

import (
	"fmt"
	"github.com/cortexproject/cortex/pkg/util/spanlogger"
	"github.com/thanos-io/thanos/pkg/store/storepb"
)

func (u *BucketStores) Query(req, srv storepb.Store_SeriesServer) error {
	spanLog, spanCtx := spanlogger.New(srv.Context(), "BucketStores.Series")
	defer spanLog.Span.Finish()

	userID := getUserIDFromGRPCContext(spanCtx)
	if userID == "" {
		return fmt.Errorf("no userID")
	}

	store := u.getStore(userID)
	if store == nil {
		return nil
	}

}
