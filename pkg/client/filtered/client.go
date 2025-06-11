package filtered

import (
	"context"
	"fmt"

	errors2 "github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type Reader struct {
	reader  client.Reader
	gvk     schema.GroupVersionKind
	filters [][]string
}

func NewReader(reader client.Reader, gvk schema.GroupVersionKind, filters [][]string) Reader {
	return Reader{
		reader:  reader,
		gvk:     gvk,
		filters: filters,
	}
}

func (r *Reader) Get(ctx context.Context, nsName types.NamespacedName) (unstructured.Unstructured, error) {
	obj := unstructured.Unstructured{}
	obj.SetGroupVersionKind(r.gvk)
	err := r.reader.Get(ctx, client.ObjectKey{Name: nsName.Name, Namespace: nsName.Namespace}, &obj)
	if err != nil {
		return unstructured.Unstructured{}, err
	}

	obj.Object = r.filterObject(obj.Object)
	return obj, nil
}

func (r *Reader) List(ctx context.Context, opts ...client.ListOption) (unstructured.UnstructuredList, error) {
	list := unstructured.UnstructuredList{Items: []unstructured.Unstructured{}}

	list.SetGroupVersionKind(r.gvk)
	err := r.reader.List(ctx, &list, opts...)
	if err != nil {
		return unstructured.UnstructuredList{}, errors2.Wrap(err, fmt.Sprintf("failed to list e.gvk [%s]", r.gvk.String()))
	}

	for index, item := range list.Items {
		list.Items[index].Object = r.filterObject(item.Object)
	}

	return list, nil
}

func (r *Reader) filterObject(obj map[string]interface{}) map[string]interface{} {
	filteredObj := make(map[string]interface{})
	for _, field := range r.filters {
		val := getNested(obj, field...)
		setNested(filteredObj, val, field...)
	}
	return filteredObj
}

func setNested(obj map[string]interface{}, val any, keys ...string) {
	if obj == nil || len(keys) == 0 {
		return
	}

	if len(keys) == 1 {
		obj[keys[0]] = val
		return
	}

	if obj[keys[0]] == nil {
		obj[keys[0]] = make(map[string]interface{})
	}

	nestedObj := obj[keys[0]].(map[string]interface{})
	setNested(nestedObj, val, keys[1:]...)
}

func getNested(obj map[string]interface{}, keys ...string) any {
	var t any
	if obj == nil || len(keys) == 0 {
		return t
	}

	if len(keys) == 1 {
		return obj[keys[0]]
	}

	switch v := obj[keys[0]].(type) {
	case map[string]interface{}:
		if len(keys) > 1 {
			return getNested(v, keys[1:]...)
		}
	}

	return t
}
