<?php
namespace go1\util_index;

class ElasticScriptBuilder
{
    private $script;
    private $params;
    private $scriptedUpsert = false;
    private $upsert = [];

    public static function create(): ElasticScriptBuilder
    {
        $_ = new ElasticScriptBuilder;
        $_->script = [];
        $_->params = [];

        return $_;
    }

    public function setUpsert(array $upsert = [])
    {
        $this->upsert = $upsert;
        return $this;
    }

    public function setScriptedUpsert(bool $op = true)
    {
        $this->scriptedUpsert = $op;
        return $this;
    }

    public function setFieldDefaultValue(string $field, $value)
    {
        $this->script[] = <<<JAVA
    if (ctx._source.$field == null) {
    	ctx._source.$field = params.$field;
	}
JAVA;
        $this->params[$field] = $value;
        return $this;
    }

    public function setField(string $field, $value)
    {
        $this->script[] = "ctx._source.$field=_$field;";
        $this->params["_$field"] = $value;
        return $this;
    }

    public function increase(string $field)
    {
        $this->script[] = <<<JAVA
    if (ctx._source.$field == null) {
    	ctx._source.$field = 0;
	}
	ctx._source.$field++;
JAVA;
        return $this;
    }

    public function decrease(string $field)
    {
        $this->script[] = <<<JAVA
    if (ctx._source.$field == null) {
    	ctx._source.$field = 0;
	}
	
	if (ctx._source.$field > 0) {
	    ctx._source.$field--;
	}
JAVA;

        return $this;
    }

    public function listAppend(string $field, $value)
    {
        $paramKey = str_replace(".", "_", $field);
        $this->script[] = <<<JAVA
    if (ctx._source.$field == null) {
    	ctx._source.$field = [];
	}
	if (ctx._source.$field.indexOf(params.$paramKey) < 0) {
        ctx._source.$field.add(params.$paramKey);
    }
JAVA;

        $this->params[$paramKey] = $value;
        return $this;
    }

    public function listRemove(string $field, $value)
    {
        $paramKey = str_replace(".", "_", $field);
        $this->script[] = <<<JAVA
    if (ctx._source.$field == null) {
    	ctx._source.$field = [];
	}
	if (ctx._source.$field.indexOf(params.$paramKey) >= 0) {
        ctx._source.$field.remove(ctx._source.$field.indexOf(params.$paramKey));
    }
JAVA;
        $this->params["$paramKey"] = $value;
        return $this;
    }

    /**
     * Example
     *
     *      ctx._source.FIELD = [
     *          PROPERTY_1: [],
     *          PROPERTY_2: []
     *      ]
     */
    public function initNestedList(string $field, array $properties)
    {
        $fields = [];
        foreach ($properties as $name) {
            $fields[] = "'$name':[]";
        }

        $fields = implode(",", $fields);
        $this->script[] = <<<JAVA
    if (ctx._source.$field instanceof ArrayList) { ctx._source.$field = null; }
    if (ctx._source.$field == null) {
    	ctx._source.$field = [
    	    $fields
    	]
	}
JAVA;
        return $this;
    }

    public function getScript(): array
    {
        $script = [
            'script' => array_filter([
                'inline' => implode("\n", $this->script),
                'params' => $this->params,
            ])
        ];

        if ($this->scriptedUpsert) {
            $script['scripted_upsert'] = true;
        }

        if ($this->upsert) {
            $script['upsert'] = $this->upsert;
        }

        return $script;
    }
}
